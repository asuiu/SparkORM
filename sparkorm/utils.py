# Author: <andrei.suiu@gmail.com>
import re
from datetime import date, datetime
from numbers import Number
from typing import Any, Dict, Iterable, Sequence, Union

from _decimal import Decimal
from pyspark.sql import Column, SparkSession
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    _all_atomic_types,
    _parse_datatype_string,
)
from tsx import TS
from tsx.ts import BaseTS

from sparkorm.base_field import PARTITIONED_BY_KEY
from sparkorm.fields import SPARK_TO_ORM_TYPE
from sparkorm.metadata_types import DBConfig

DECIMAL_TYPE_RE = re.compile(r"decimal\((\d+),(\d+)\)", re.I)
SqlPrimitive = Union[str, Number, bool, Decimal, None]
SqlType = Union[SqlPrimitive, Iterable[SqlPrimitive]]


def spark_struct_to_sql_string(spark_struct: StructField) -> str:
    data_type = spark_struct.dataType
    name = spark_struct.name
    nullable = "" if spark_struct.nullable else " NOT NULL"
    sql_type_string = spark_type_to_sql_type(data_type)
    return f"{name} {sql_type_string}{nullable}"


def spark_type_to_sql_type(data_type: DataType) -> str:
    type_mapping = {
        StringType: "STRING",
        BooleanType: "BOOLEAN",
        IntegerType: "INT",
        FloatType: "FLOAT",
        DoubleType: "DOUBLE",
        DateType: "DATE",
        TimestampType: "TIMESTAMP",
        LongType: "BIGINT",
        ShortType: "SMALLINT",
        ByteType: "TINYINT",
        BinaryType: "BINARY",
    }
    if isinstance(data_type, ArrayType):
        element_sql_type = spark_type_to_sql_type(data_type.elementType)
        return f"ARRAY<{element_sql_type}>"
    if isinstance(data_type, MapType):
        key_type = spark_type_to_sql_type(data_type.keyType)
        value_type = spark_type_to_sql_type(data_type.valueType)
        return f"MAP<{key_type},{value_type}>"
    if isinstance(data_type, DecimalType):
        return f"DECIMAL({data_type.precision},{data_type.scale})"
    if isinstance(data_type, StructType):
        fields = [f"{field.name}: {spark_type_to_sql_type(field.dataType)}" for field in data_type.fields]
        return f"STRUCT<{', '.join(fields)}>"

    for spark_type_class, sql_type_string in type_mapping.items():
        if isinstance(data_type, spark_type_class):
            return sql_type_string

    raise ValueError(f"Unsupported PySpark type: {type(data_type)}")


def deserialize_spark_dtype_from_string(dtype: str) -> DataType:
    """
    Be aware that if the table contains Map or Array types,
        this function requires real SparkSession and it will fail with mocked sessions used by _parse_datatype_string().
    """

    if dtype in _all_atomic_types:
        return _all_atomic_types[dtype]()
    if dtype.startswith("decimal"):
        m = DECIMAL_TYPE_RE.match(dtype)
        precision = int(m.group(1))
        scale = int(m.group(2))
        dtype = DecimalType(precision, scale)
        return dtype
    dtype = _parse_datatype_string(dtype)
    return dtype


def map_column_to_struct_field(column: Column) -> StructField:
    dataType = deserialize_spark_dtype_from_string(column.dataType)
    meta_data = {PARTITIONED_BY_KEY: column.isPartition} if column.isPartition else None
    return StructField(
        name=column.name,
        dataType=dataType,
        nullable=column.nullable,
        metadata=meta_data,
    )


def convert_to_struct_type(table_columns: Sequence[Column]) -> StructType:
    return StructType([map_column_to_struct_field(column) for column in table_columns])


def to_camel_case(table_name: str) -> str:
    return "".join(word.capitalize() for word in table_name.split("_"))


# pylint: disable=dangerous-default-value
def create_model_code(spark: SparkSession, db_name: str, table_name: str, db_config_map: Dict[str, DBConfig] = {None: None}) -> str:
    """
    Be aware that if the table contains Map or Array types, this function requires real SparkSession and it will fail with mocked sessions.
    """
    class_name = to_camel_case(table_name)
    db_name_prefix = f"{db_name}." if db_name else ""
    table_columns = spark.catalog.listColumns(tableName=f"{db_name_prefix}{table_name}")
    struct_type = convert_to_struct_type(table_columns)

    field_reprs = []
    for field in struct_type.fields:
        orm_type = SPARK_TO_ORM_TYPE[type(field.dataType)]
        orm_field = orm_type.from_spark_struct_field(field, use_name=False)
        field_repr = f"   {field.name} = {repr(orm_field)}"
        field_reprs.append(field_repr)

    db_config = db_config_map[db_name]
    if db_config is not None:
        db_config_val = f"db_config = {db_config.__name__}"
    else:
        db_config_val = ""
    meta_repr = f"""   class Meta(MetaConfig):\n       name = "{table_name}"\n"""
    if db_config_val:
        meta_repr += f"       {db_config_val}\n"
    class_fields = "\n".join(field_reprs)
    class_template = f"""
class {class_name}(TableModel):
{meta_repr}
{class_fields}
"""

    return class_template


def as_sql_type(data_type: DataType) -> str:
    """
    This will convert a PySpark data type to a SQL type.
    """
    return spark_type_to_sql_type(data_type)


def as_sql_value(value: SqlType) -> str:  # pylint: disable=too-many-return-statements
    if value is None:
        return "NULL"
    if isinstance(value, datetime):
        # according to official documentation: https://spark.apache.org/docs/3.5.0/sql-ref-literals.html#timestamp-syntax
        return f"TIMESTAMP '{value}'"
    if isinstance(value, date):
        # according to official documentation: https://spark.apache.org/docs/3.5.0/sql-ref-literals.html#datetime-literal
        return f"DATE '{value}'"
    if isinstance(value, BaseTS):
        # according to official documentation: https://spark.apache.org/docs/3.5.0/sql-ref-literals.html#timestamp-syntax
        return f"TIMESTAMP '{value.isoformat()}'"
    if isinstance(value, str):
        if "'" in value:
            encoded_quotes = value.replace("'", "\\'")
            return f"'{encoded_quotes}'"
        return f"'{value}'"
    if isinstance(value, bool):
        return "True" if value else "False"
    if isinstance(value, Number):
        return str(value)
    if isinstance(value, Iterable):
        return f"({','.join(as_sql_value(v) for v in value)})"
    return str(value)


# pylint: disable=too-many-return-statements, too-many-branches
def get_spark_type(value: Any, spark_type: DataType) -> Any:
    if value is None:
        return None
    if isinstance(spark_type, StringType):
        return str(value)
    if isinstance(spark_type, BooleanType):
        return bool(value)
    if isinstance(spark_type, IntegerType):
        return int(value)
    if isinstance(spark_type, FloatType):
        return float(value)
    if isinstance(spark_type, DoubleType):
        return float(value)
    if isinstance(spark_type, DateType):
        return TS(value).as_dt().date()
    if isinstance(spark_type, TimestampType):
        if isinstance(value, (Number, Decimal, str)):
            return TS(value).as_dt()
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime(value.year, value.month, value.day)
        return value
    if isinstance(spark_type, LongType):
        return int(value)
    if isinstance(spark_type, ShortType):
        return int(value)
    if isinstance(spark_type, ByteType):
        return int(value)
    if isinstance(spark_type, BinaryType):
        return bytes(value)
    return value
