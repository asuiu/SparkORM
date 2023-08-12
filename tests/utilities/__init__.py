"""Utilities to support testing; tests do not live under here."""
from contextlib import contextmanager
from typing import Sequence, Collection, Any

from _decimal import Decimal
from pyspark.sql.types import StructType, DataType, TimestampType, DateType, DecimalType
from tsx import TS


@contextmanager
def does_not_raise():
    """For use in pytest"""
    yield


def convert_to_spark_types(rows: Sequence[Collection[Any]], schema: StructType):
    def _convert_element_to_spark_type(x: Any, _type: DataType):
        if isinstance(_type, TimestampType):
            return TS(x).as_dt()
        if isinstance(_type, DateType):
            return TS(x).as_dt().date()
        if isinstance(_type, DecimalType):
            return Decimal(x)
        return x

    return [[_convert_element_to_spark_type(x, schema[i].dataType) for i, x in enumerate(row)] for row in rows]


__all__ = ["does_not_raise", "convert_to_spark_types"]
