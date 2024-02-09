# Author: <andrei.suiu@gmail.com>
import csv
from typing import IO, Any, Iterable, List, Literal, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import Row, StructType
from streamerate import stream

from sparkorm.base_field import PARTITIONED_BY_KEY
from sparkorm.exceptions import TableUpdateError
from sparkorm.metadata_types import (
    DBConfig,
    DropAndCreateStrategy,
    LocationConfig,
    LocationType,
    MetaConfig,
    NoChangeStrategy,
    SchemaMigrationStrategy,
    SchemaUpdateStatus,
)
from sparkorm.struct import Struct
from sparkorm.utils import convert_to_struct_type, get_spark_type


class BaseModel(Struct):
    VALID_METADATA_ATTRS = {"name", "db_config", "migration_strategy", "includes", "location"}
    SQL_NAME = "NAME"

    class Meta(MetaConfig):
        pass

    def __init__(self, spark: SparkSession):
        super().__init__()
        self._spark = spark

    def __init_subclass__(cls, /, **kwargs):
        if hasattr(cls, "Meta"):
            attributes = {k: v for k, v in vars(cls.Meta).items() if not callable(v) and not k.startswith("_")}
            assert set(attributes.keys()).issubset(cls.VALID_METADATA_ATTRS), f"Invalid attributes: {attributes.keys()}"
        super().__init_subclass__(**kwargs)

    @classmethod
    def get_spark_schema(cls) -> StructType:
        return cls._valid_struct_metadata().spark_struct

    @classmethod
    def get_full_name(cls) -> str:
        name = cls.get_name()
        db_name = cls.get_db_name()
        if db_name is None:
            return name
        return db_name + "." + name

    @classmethod
    def get_db_name(cls) -> Optional[str]:
        assert hasattr(cls, "Meta"), f"Class {cls} must have Meta class"
        if not hasattr(cls.Meta, "db_config") or cls.Meta.db_config is None:
            return None
        db_config = cls.Meta.db_config
        assert issubclass(db_config, DBConfig)
        return db_config.get_name()

    @classmethod
    def get_name(cls) -> str:
        assert hasattr(cls, "Meta"), f"Class {cls} must have Meta class"
        return cls.Meta.name

    @classmethod
    def _get_migration_strategy(cls) -> SchemaMigrationStrategy:
        """Default migration strategy is NoChangeStrategy"""
        if not hasattr(cls.Meta, "migration_strategy"):
            return NoChangeStrategy()
        assert isinstance(cls.Meta.migration_strategy, SchemaMigrationStrategy)
        return cls.Meta.migration_strategy

    @classmethod
    def get_migration_strategy(cls) -> SchemaMigrationStrategy:
        return cls._get_migration_strategy()

    def sql(self, sqlQuery: str, *args, **kwargs: Any) -> DataFrame:
        """
        Execute a SQL query and return the result as a DataFrame.
        """
        return self._spark.sql(sqlQuery, *args, **kwargs)


class TableModel(BaseModel):
    def ensure_exists(self) -> SchemaUpdateStatus:
        """
        Ensure that the table exists in the database, and validate its structure.
        If the table does not exist, it will be created, and the method will return False.
        If the table exists, but has a different structure, an exception will be raised.
        Returns True if the table already exists and has the correct structure.
        """
        full_name = self.get_full_name()
        spark_schema = self.get_spark_schema()

        if self._spark.catalog.tableExists(tableName=self.get_name(), dbName=self.get_db_name()):
            if hasattr(self.Meta, "location") and self.Meta.location is not None:
                self.create(or_replace=True)
                return SchemaUpdateStatus.REPLACED
            table_columns = self._spark.catalog.listColumns(tableName=self.get_name(), dbName=self.get_db_name())
            struct_type = convert_to_struct_type(table_columns)
            if struct_type != spark_schema:
                migration_strategy = self._get_migration_strategy()
                if isinstance(migration_strategy, DropAndCreateStrategy):
                    self.drop()
                    self.create(or_replace=False)
                    return SchemaUpdateStatus.DROPPED_AND_CREATED
                raise TableUpdateError(
                    f"Table {full_name} already exists with different schema. " f"Existing schema: {struct_type}, " f"Expected schema: {spark_schema}"
                )
            return SchemaUpdateStatus.SKIPPED

        self.create()
        return SchemaUpdateStatus.CREATED

    def create(self, or_replace: bool = False) -> None:
        """
        Raises exception if the table already exists.
        """
        full_name = self.get_full_name()
        if hasattr(self.Meta, "location"):
            location = self.Meta.location
        else:
            location = None
        if location is not None:
            assert isinstance(location, LocationConfig), f"Invalid location: {location}"
            assert isinstance(location.type, LocationType), f"Invalid location type: {location.type}"
            assert isinstance(location.location, str), f"Invalid location: {location.location}"
            location_type = location.type
            location_str = location.location
            or_replace_str = " OR REPLACE" if or_replace else ""
            create_statement = f"CREATE{or_replace_str} TABLE {full_name} USING {location_type} LOCATION '{location_str}'"
            self._spark.sql(create_statement)
            return
        spark_schema = self.get_spark_schema()

        fields = spark_schema.fields
        field_defs = self.sql_col_def()
        create_statement = f"CREATE TABLE {full_name} ({field_defs})"
        partitioned_by_fields = [field.name for field in fields if field.metadata.get(PARTITIONED_BY_KEY, False) is True]
        if partitioned_by_fields:
            create_statement += f" PARTITIONED BY ({','.join(partitioned_by_fields)})"

        self._spark.sql(create_statement)

    def truncate(self) -> None:
        full_name = self.get_full_name()
        self._spark.sql(f"TRUNCATE TABLE {full_name}")

    def drop(self) -> None:
        full_name = self.get_full_name()
        self._spark.sql(f"DROP TABLE {full_name}")

    def insert(self, values: Iterable[Sequence], batch_size: int = 500) -> None:
        """
        Insert SQL expressions into the table. Attention, the values is an Iterable of rows which contain valid SQL expressions.
        Example:
            values = [('"A"', 'CURRENT_DATE', '12.0'), ('"B"', 'DATE("2012-01-01")', '13.0')]
        Thus the strings will be quoted as SQL expressions, and the CURRENT_DATE and DATE("2012-01-01") will be inserted as is.

        :param batch_size: Number of rows to insert in a single insert statement
        """
        full_name = self.get_full_name()
        column_names = ",".join([x.name for x in self.get_spark_schema().fields])
        batches = stream(values).batch(batch_size)
        for batch in batches:
            serialized_batch = batch.map(lambda row: f'({",".join(row)})').mkString(",")
            insert_statement = f"INSERT INTO {full_name} ( {column_names} ) VALUES {serialized_batch}"
            self._spark.sql(insert_statement)

    def insert_from_csv(self, f: IO, batch_size: int = 500) -> None:
        """
        Insert CSV data into the table.
        :param f: File object containing the CSV data
        :param batch_size: Number of rows to insert in a single insert statement
        """
        csv_reader = csv.DictReader(f)
        # Ensure that all columns in the order list are present in the CSV file
        columns_order = [x.name for x in self.get_spark_schema().fields]
        for column in columns_order:
            if column not in csv_reader.fieldnames:
                raise ValueError(f"Column '{column}' not found in the CSV file.")

        ordered_rows = stream(csv_reader).map(lambda row: [row[column] for column in columns_order])
        self.insert(ordered_rows, batch_size)

    def insert_from_select(self, select_statement: str) -> DataFrame:
        full_name = self.get_full_name()
        insert_statement = f"INSERT INTO {full_name} {select_statement}"
        return self._spark.sql(insert_statement)

    def insert_from_df(self, df: DataFrame, saveMode: Literal["append", "overwrite", "ignore", "error"] = "error") -> None:
        full_name = self.get_full_name()
        return df.write.mode(saveMode).insertInto(full_name)

    def as_df(self) -> DataFrame:
        full_name = self.get_full_name()
        return self._spark.table(full_name)

    def to_spark_rows(self, rows: Sequence[Sequence[Any]]) -> List[Row]:
        """
        This will convert a Row with common data types to Spark types depending on the schema of the tableModel

        Note: starting with PySpark 3, the Row class ignores the names of the arguments and takes solely the order of the arguments, so we need to order cols
        """
        spark_schema = self.get_spark_schema()
        field_names = spark_schema.fieldNames()
        spark_rows = []
        for row in rows:
            spark_row = []
            for field, value in zip(field_names, row):
                field = field.dataType(get_spark_type(value, field.dataType))
                spark_row.append(field)
            spark_rows.append(Row(*spark_row))

        return spark_rows


class ViewModel(BaseModel):
    def create_or_replace(self, select_statement: str) -> None:
        full_name = self.get_full_name()
        create_statement = f"CREATE OR REPLACE VIEW {full_name} AS ({select_statement})"
        self._spark.sql(create_statement)
