"""Exceptions for this package."""
from typing import List, Any


class FieldNameError(Exception):
    """A field name management error occurred."""


class FieldParentError(Exception):
    """A field-to-field-parent management error occurred."""


class InvalidStructError(Exception):
    """Invalid creation of a custom Struct subclass."""


class StructImplementationError(InvalidStructError):
    """A custom Struct subclass fails to correctly implement required fields."""


class InvalidDataFrameError(Exception):
    """A DataFrame does not match a schema."""


class FieldValueValidationError(Exception):
    """Value is not valid for a field."""


class StructInstantiationError(Exception):
    """Base exception for errors when attempting to instantiate data from a schema."""


class StructInstantiationArgumentsError(StructInstantiationError):
    """Incorrect arguments specified when creating a data object from a schema; e.g., a required argument is missing."""

    def __init__(
        self,
        properties: List[str],
        unfilled_properties: List[str],
        duplicate_properties: List[str],
        surplus_positional_values: List[Any],
        surplus_keyword_args: List[str],
    ):
        msg = ""
        if duplicate_properties:
            msg += (
                "There were struct properties with multiple values. Repeated properties: "
                + ", ".join(duplicate_properties)
                + " \n"
            )
        if surplus_positional_values:
            msg += (
                f"There were {len(surplus_positional_values)} surplus positional arguments. Values for surplus args: "
                + ", ".join(map(str, surplus_positional_values))
                + " \n"
            )
        if surplus_keyword_args:
            msg += "There were surplus keyword arguments: " + ", ".join(surplus_keyword_args) + " \n"
        msg += "Properties required by this struct are: " + ", ".join(properties)
        if unfilled_properties:
            msg += "\nOmitted struct properties were defaulted to null: " + ", ".join(unfilled_properties)
        super().__init__(msg)


class StructInstantiationArgumentTypeError(StructInstantiationError):
    """
    Found an argument with an incorrect type when creating a data object from a schema.

    For example using a null in a non-nullable or using a string as a float.
    """


class TableUpdateError(Exception):
    """The table already exists, but with a different structure."""
