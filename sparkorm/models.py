# Author: <andrei.suiu@gmail.com>
from abc import ABC, abstractmethod
from typing import Sequence, Optional, Iterable

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
)
from streamerate import stream

from sparkorm import Struct
from sparkorm.exceptions import TableUpdateError
from sparkorm.fields.base import PARTITIONED_BY_KEY
from sparkorm.utils import spark_struct_to_sql_string, convert_to_struct_type


class DBConfig(ABC):
    @classmethod
    @abstractmethod
    def get_name(cls) -> str:
        ...


class BaseModel(Struct):
    def __init__(self, spark: SparkSession):
        super().__init__()
        self._spark = spark

    def __init_subclass__(cls, /, **kwargs):
        if hasattr(cls, "Meta"):
            attributes = {k: v for k, v in vars(cls.Meta).items() if not callable(v) and not k.startswith("__")}
            valid_attrs = {"name", "db_config"}
            assert set(attributes.keys()).issubset(valid_attrs), f"Invalid attributes: {attributes.keys()}"
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
        if not hasattr(cls.Meta, "db_config"):
            return None
        db_config = cls.Meta.db_config
        assert issubclass(db_config, DBConfig)
        return db_config.get_name()

    @classmethod
    def get_name(cls) -> str:
        assert hasattr(cls, "Meta"), f"Class {cls} must have Meta class"
        return cls.Meta.name


class TableModel(BaseModel):
    def ensure_exists(self) -> bool:
        """
        Ensure that the table exists in the database, and validate its structure.
        If the table does not exist, it will be created, and the method will return False.
        If the table exists, but has a different structure, an exception will be raised.
        Returns True if the table already exists and has the correct structure.
        """
        full_name = self.get_full_name()
        spark_schema = self.get_spark_schema()

        if self._spark.catalog.tableExists(tableName=self.get_name(), dbName=self.get_db_name()):
            table_columns = self._spark.catalog.listColumns(tableName=self.get_name(), dbName=self.get_db_name())
            struct_type = convert_to_struct_type(table_columns)
            if struct_type != spark_schema:
                raise TableUpdateError(
                    f"Table {full_name} already exists with different schema. "
                    f"Existing schema: {struct_type}, "
                    f"Expected schema: {spark_schema}"
                )
            return True
        else:
            self.create()
            return False

    def create(self) -> None:
        """
        Raises exception if the table already exists.
        """
        full_name = self.get_full_name()
        spark_schema = self.get_spark_schema()

        fields = spark_schema.fields
        column_names = stream(fields).map(spark_struct_to_sql_string).mkString(",")
        create_statement = f"CREATE TABLE {full_name} ({column_names})"
        partitioned_by_fields = [
            field.name for field in fields if field.metadata.get(PARTITIONED_BY_KEY, False) is True
        ]
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


class ViewModel(BaseModel):
    def create_or_replace(self, select_statement: str) -> None:
        full_name = self.get_full_name()
        create_statement = f"CREATE OR REPLACE VIEW {full_name} AS ({select_statement})"
        self._spark.sql(create_statement)
