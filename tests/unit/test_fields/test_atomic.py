import pytest
from pyspark.sql.types import (
    ByteType,
    IntegerType,
    LongType,
    ShortType,
    DecimalType,
    DoubleType,
    FloatType,
    StringType,
    BinaryType,
    BooleanType,
    DateType,
    TimestampType,
)

from sparkorm.fields.atomic import (
    Byte,
    Integer,
    Long,
    Short,
    Decimal,
    Double,
    Float,
    String,
    Binary,
    Boolean,
    Date,
    Timestamp,
)


@pytest.mark.parametrize(
    "sparkorm_field_class, spark_type_class",
    [
        [Byte, ByteType],
        [Integer, IntegerType],
        [Long, LongType],
        [Short, ShortType],
        [Decimal, DecimalType],
        [Double, DoubleType],
        [Float, FloatType],
        [String, StringType],
        [Binary, BinaryType],
        [Boolean, BooleanType],
        [Date, DateType],
        [Timestamp, TimestampType],
    ],
)
def test_atomics_have_correct_spark_type_classes(sparkorm_field_class, spark_type_class):
    field_instance = sparkorm_field_class()
    assert field_instance._spark_type_class is spark_type_class


@pytest.mark.parametrize(
    "sparkorm_field, spark_type",
    [
        [Decimal(), DecimalType(10, 0)],
        [Decimal(12), DecimalType(12, 0)],
        [Decimal(12, 5), DecimalType(12, 5)],
    ],
)
def test_decimal_precision_and_scale(sparkorm_field, spark_type):
    assert sparkorm_field._spark_data_type == spark_type
