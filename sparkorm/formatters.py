"""String formatters."""

from pyspark.sql.types import StructType, StructField, ArrayType, DataType


class SparkSchemaPrettifier:
    """Pretty formatting of a Spark schema."""

    PREFIX = "    "

    #
    # Entry point

    @classmethod
    def pretty_schema(cls, struct: StructType) -> str:
        """Prettify `struct`."""
        return cls._pretty_struct_type(struct, depth=0)

    #
    # Helpers

    @classmethod
    def _indent(cls, depth: int) -> str:
        """
        Create indentation prefix for given depth.

        "depth" is defined as the current nestedness within StructTypes. The root (i.e., top level
        StructType) has a depth of zero.
        """
        return cls.PREFIX * (depth + 1)

    @classmethod
    def _boolean_as_str(cls, value: bool) -> str:
        return str(value)

    #
    # Recursive handlers

    @classmethod
    def _pretty_data_type(cls, dtype: DataType, depth: int) -> str:
        """Handle an instance of a data type."""
        if isinstance(dtype, StructType):
            # off load to the struct type handler
            return cls._pretty_struct_type(dtype, depth + 1)
        if isinstance(dtype, ArrayType):
            # off load to the array type handler
            return cls._pretty_array_type(dtype, depth)

        # for all other data types, give the type name
        return f"{dtype.__class__.__name__}()"

    @classmethod
    def _pretty_struct_type(cls, struct: StructType, depth: int) -> str:
        """Handle the case where the data type is a struct type."""
        assert isinstance(struct, StructType)
        if not struct.fields:
            formatted = "StructType([])"
        else:
            delim = "\n" + cls._indent(depth)
            lines = []
            for field in struct.fields:
                lines.append(f"{delim}{cls._pretty_struct_field(field, depth)}")
            joined_lines = ", ".join(lines)
            formatted = f"StructType([{joined_lines}])"
        return formatted

    @classmethod
    def _pretty_array_type(cls, array: ArrayType, depth: int) -> str:
        """Handle the case where the data type is an array type."""
        assert isinstance(array, ArrayType)
        # also print out the `containsNull` ArrayType boolean

        # pylint: disable=consider-using-f-string
        return "ArrayType({}, {})".format(
            cls._pretty_data_type(array.elementType, depth),
            cls._boolean_as_str(array.containsNull),
        )

    @classmethod
    def _pretty_struct_field(cls, field: StructField, depth: int) -> str:
        """Helper for `_pretty_struct_type`, that formats a field."""
        assert isinstance(field, StructField)
        formatted = f"StructField('{field.name}', "
        if isinstance(field.dataType, (StructType, ArrayType)):
            formatted += "\n" + cls._indent(depth + 1)

        formatted += cls._pretty_data_type(field.dataType, depth + 1) + ", "

        if isinstance(field.dataType, (StructType, ArrayType)):
            formatted += "\n" + cls._indent(depth + 1)

        formatted += f"{cls._boolean_as_str(field.nullable)}"

        if field.metadata:
            formatted += ", "
            if isinstance(field.dataType, (StructType, ArrayType)):
                formatted += "\n" + cls._indent(depth + 1)
            formatted += str(field.metadata)

        formatted += ")"

        return formatted


def pretty_schema(struct: StructType) -> str:
    """
    Returns a pretty stringified `struct`.

    This attempts to mimic StructType's default string formatter, but with the
    addition of white space, new lines, and indentation.
    """
    return SparkSchemaPrettifier.pretty_schema(struct)
