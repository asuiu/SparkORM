"""Schema management operations."""

from typing import Union, Type, TYPE_CHECKING

from pyspark.sql.types import StructType

if TYPE_CHECKING:
    from sparkorm import Struct


def schema(schema_object: Union["Struct", Type["Struct"]]) -> StructType:
    """Spark schema for a struct object."""
    return schema_object._valid_struct_metadata().spark_struct  # pylint: disable=protected-access
