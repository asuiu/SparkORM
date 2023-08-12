# Author: <andrei.suiu@gmail.com>
import re
from typing import Sequence

from pyspark.sql import Column
from pyspark.sql.types import (
    StructField,
    DataType,
    StringType,
    BooleanType,
    IntegerType,
    FloatType,
    DoubleType,
    DateType,
    TimestampType,
    LongType,
    ShortType,
    ByteType,
    BinaryType,
    ArrayType,
    MapType,
    DecimalType,
    StructType,
    _all_atomic_types,
)

from sparkorm.fields.base import PARTITIONED_BY_KEY

DECIMAL_TYPE_RE = re.compile(r"decimal\((\d+),(\d+)\)", re.I)


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
    if dtype in _all_atomic_types:
        return _all_atomic_types[dtype]()
    if dtype.startswith("decimal"):
        m = DECIMAL_TYPE_RE.match(dtype)
        precision = int(m.group(1))
        scale = int(m.group(2))
        dtype = DecimalType(precision, scale)
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
