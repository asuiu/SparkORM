"""Array field."""

import copy
from collections.abc import Sequence
from typing import Optional, Generic, TypeVar, Any, Dict, Type, cast, TYPE_CHECKING

from pyspark.sql.types import ArrayType, StructField

from sparkorm.exceptions import FieldValueValidationError
from sparkorm.fields.base import BaseField

if TYPE_CHECKING:
    from sparkorm import Struct


ArrayElementType = TypeVar("ArrayElementType", bound=BaseField)  # pylint: disable=invalid-name


class Array(Generic[ArrayElementType], BaseField):
    """
    Array field; shadows ArrayType in the Spark API.

    A spark schema generated with this array field will behave as follows:
    - Nullability of the `StructField` is given by `Array.nullable`, as per normal.
    - `containsNull` of the `ArrayType` is given by the nullability of the element contained
      within this field. In other words, `ArrayType.containsNull` is given by
      `Array.element.nullable`.

    Attributes:
        e:
            Data type info for the element of this array. Should be an instance of a `BaseField`.
            Received in constructor as "element".
    """

    __hash__ = BaseField.__hash__

    e: ArrayElementType  # pylint: disable=invalid-name

    def __init__(
        self,
        element: ArrayElementType,
        nullable: bool = True,
        name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(nullable=nullable, name=name, metadata=metadata)

        if not isinstance(element, BaseField):
            raise ValueError(f"Array element must be a field. Found type: {type(element).__name__}")

        if element._resolve_field_name() is not None:
            # we require that the array's element should be a "vanilla" field. it should not have been given
            # any name (explicit nor contextual)
            raise ValueError(
                "When using a field as the element field of an array, the field should not have a name. "
                f"The field's name resolved to: {element._resolve_field_name()}"
            )

        # hand down this array's explicit name to its child element
        # this is to ensure correct naming in path chaining (see `self._replace_parent` and `path_seq`)
        element = cast(ArrayElementType, element._replace_explicit_name(name=self._explicit_name))
        self.e = element  # pylint: disable=invalid-name

    #
    # Field path chaining

    def _replace_parent(self, parent: Optional["Struct"] = None) -> "Array[ArrayElementType]":
        """Return a copy of this array with the parent attribute set."""
        self_copy = copy.copy(self)
        self_copy._parent_struct = parent  # pylint: disable=protected-access
        self_copy.e = cast(ArrayElementType, self.e._replace_parent(parent=parent))  # pylint: disable=protected-access
        return self_copy

    #
    # Field name management

    def _set_contextual_name(self, value: str) -> None:
        super()._set_contextual_name(value)
        # set child to same name as parent; i.e., propagate contextual name downwards:
        self.e._set_contextual_name(value)  # pylint: disable=protected-access

    #
    # Pass through to the element, for users who don't want to use the `.e` field

    def __getattribute__(self, attr_name: str) -> Any:
        """Custom get attirubte behaviour."""
        if attr_name.startswith("_"):
            return super().__getattribute__(attr_name)

        try:
            attr_value = super().__getattribute__(attr_name)
        except AttributeError:
            attr_value = None

        if attr_value is not None:
            return attr_value

        return getattr(super().__getattribute__("e"), attr_name)

    #
    # Spark type management

    @property
    def _spark_type_class(self) -> Type[ArrayType]:
        return ArrayType

    @property
    def _spark_struct_field(self) -> StructField:
        """The Spark StructField for this field."""
        # containsNull => is used to indicate if elements in a ArrayType value can have null values
        return StructField(
            name=self._field_name,
            dataType=ArrayType(
                # Note that we do not care about the element's field name here:
                elementType=self.e._spark_struct_field.dataType,  # pylint: disable=protected-access
                containsNull=self.e._is_nullable,  # pylint: disable=protected-access
            ),
            nullable=self._is_nullable,
            metadata=self._metadata,
        )

    def _validate_on_value(self, value: Any) -> None:
        super()._validate_on_value(value)
        if value is None:
            # super() will have already validate none vs nullability. if None, then it's safe to be none
            return
        if not isinstance(value, Sequence):
            raise FieldValueValidationError(f"Value for an array must be a sequence, not '{type(value).__name__}'")
        if isinstance(value, str):
            raise FieldValueValidationError(
                f"Value for an array must not be a string. Found value '{value}'. Did you mean to use a list of "
                "strings?"
            )
        for item in value:
            element_field = self.e
            if not element_field._is_nullable and item is None:  # pylint: disable=protected-access
                # to improve readability for errors, we preemptively validate the non-nullability of the array
                # element here
                msg = (
                    "Encountered None value in array, but the element field of this array is specified as "
                    "non-nullable"
                )
                if self._resolve_field_name() is not None:
                    msg += f" (array field name = '{self._resolve_field_name()}')"
                raise FieldValueValidationError(msg)
            self.e._validate_on_value(item)  # pylint: disable=protected-access

    #
    # Misc

    def __eq__(self, other: Any) -> bool:
        """True if `self` equals `other`."""
        return super().__eq__(other) and isinstance(other, Array) and self.e == other.e
