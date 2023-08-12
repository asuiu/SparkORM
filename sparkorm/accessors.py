"""Accessor functions in order to access field paths, field names, and related field attributes."""

from typing import Sequence, Dict, Any

from pyspark.sql import Column
from pyspark.sql.types import StructField

from sparkorm.fields.base import BaseField


def path_seq(field: BaseField) -> Sequence[str]:
    """Return the sequence of items that constitute the path to the field `field`; an alias of `BaseField.SEQ`."""
    return field.SEQ


def path_col(field: BaseField) -> Column:
    """Return the Spark column pointing to field `field`; an alias of `BaseField.COL`."""
    return field.COL


def path_str(field: BaseField) -> str:
    """Return dot-delimited path to field `field`; an alias of `BaseField.PATH`."""
    return field.PATH


def name(field: BaseField) -> str:
    """Return field name of field `field`; an alias of `BaseField.NAME`."""
    return field.NAME


def metadata(field: BaseField) -> Dict[str, Any]:
    """Return field metadata of field `field`; an alias of `BaseField.METADATA`."""
    return field.METADATA


def struct_field(field: BaseField) -> StructField:
    """Return the equivalent PySpark StructField of field `field`."""
    return field._spark_struct_field  # pylint: disable=protected-access
