"""Concrete atomic fields."""
from datetime import datetime, date
from typing import Type, Any
import decimal

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

from sparkorm.fields.base import AtomicField, IntegralField, FractionalField, _validate_value_type_for_field


#
# DataType -> AtomicType -> NumericType -> IntegralType


class Byte(IntegralField):
    """Field for Spark's ByteType."""

    @property
    def _spark_type_class(self) -> Type[ByteType]:
        return ByteType


class Integer(IntegralField):
    """Field for Spark's IntegerType."""

    @property
    def _spark_type_class(self) -> Type[IntegerType]:
        return IntegerType


class Long(IntegralField):
    """Field for Spark's LongType."""

    @property
    def _spark_type_class(self) -> Type[LongType]:
        return LongType


class Short(IntegralField):
    """Field for Spark's ShortType."""

    @property
    def _spark_type_class(self) -> Type[ShortType]:
        return ShortType


#
# DataType -> AtomicType -> NumericType -> FractionalType


class Decimal(FractionalField):
    """Field for Spark's DecimalType."""

    __precision: int
    __scale: int

    def __init__(self, precision: int = 10, scale: int = 0, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.__precision = precision
        self.__scale = scale

    @property
    def _spark_type_class(self) -> Type[DecimalType]:
        return DecimalType

    @property
    def _spark_data_type(self) -> DecimalType:
        return DecimalType(self.__precision, self.__scale)

    def _validate_on_value(self, value: Any) -> None:
        super()._validate_on_value(value)
        _validate_value_type_for_field((decimal.Decimal,), value)


class Double(FractionalField):
    """Field for Spark's DoubleType."""

    @property
    def _spark_type_class(self) -> Type[DoubleType]:
        return DoubleType

    def _validate_on_value(self, value: Any) -> None:
        super()._validate_on_value(value)
        _validate_value_type_for_field((float,), value)


class Float(FractionalField):
    """Field for Spark's FloatType."""

    @property
    def _spark_type_class(self) -> Type[FloatType]:
        return FloatType

    def _validate_on_value(self, value: Any) -> None:
        super()._validate_on_value(value)
        _validate_value_type_for_field((float,), value)


#
# DataType -> AtomicType -> non-numeric types


class String(AtomicField):
    """Field for Spark's StringType."""

    @property
    def _spark_type_class(self) -> Type[StringType]:
        return StringType

    def _validate_on_value(self, value: Any) -> None:
        super()._validate_on_value(value)
        _validate_value_type_for_field((str,), value)


class Binary(AtomicField):
    """Field for Spark's BinaryType."""

    @property
    def _spark_type_class(self) -> Type[BinaryType]:
        return BinaryType

    def _validate_on_value(self, value: Any) -> None:
        super()._validate_on_value(value)
        _validate_value_type_for_field((bytearray,), value)


class Boolean(AtomicField):
    """Field for Spark's BooleanType."""

    @property
    def _spark_type_class(self) -> Type[BooleanType]:
        return BooleanType

    def _validate_on_value(self, value: Any) -> None:
        super()._validate_on_value(value)
        _validate_value_type_for_field((bool,), value)


class Date(AtomicField):
    """Field for Spark's DateType."""

    @property
    def _spark_type_class(self) -> Type[DateType]:
        return DateType

    def _validate_on_value(self, value: Any) -> None:
        super()._validate_on_value(value)
        _validate_value_type_for_field((date, datetime), value)


class Timestamp(AtomicField):
    """Field for Spark's TimestampType."""

    @property
    def _spark_type_class(self) -> Type[TimestampType]:
        return TimestampType

    def _validate_on_value(self, value: Any) -> None:
        super()._validate_on_value(value)
        _validate_value_type_for_field((datetime,), value)
