import copy
import decimal
from datetime import date, datetime
from typing import Any, Dict, Generic, Optional, Sequence, Tuple, Type, TypeVar, cast

from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
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
)

from sparkorm.base_field import (
    PARTITIONED_BY_KEY,
    AtomicField,
    BaseField,
    FractionalField,
    IntegralField,
    _validate_value_type_for_field,
)
from sparkorm.exceptions import FieldValueValidationError
from sparkorm.struct import Struct


class Byte(IntegralField):
    """Field for Spark's ByteType."""

    @property
    def _spark_type_class(self) -> Type[ByteType]:
        return ByteType

    def sql_type(self) -> str:
        return "TINYINT"


class Integer(IntegralField):
    """Field for Spark's IntegerType."""

    @property
    def _spark_type_class(self) -> Type[IntegerType]:
        return IntegerType

    def sql_type(self) -> str:
        return "INT"


class Long(IntegralField):
    """Field for Spark's LongType."""

    AUTO_INCREMENT_KEY = "auto_increment"

    def __init__(self, *args, auto_increment: bool = False, **kwargs):
        super().__init__(*args, **kwargs)
        if auto_increment or self._metadata.get(self.AUTO_INCREMENT_KEY, False):
            self._metadata[self.AUTO_INCREMENT_KEY] = True

        self._auto_increment = auto_increment

    @property
    def _spark_type_class(self) -> Type[LongType]:
        return LongType

    def sql_type(self) -> str:
        return "BIGINT"

    def sql_col_def(self) -> str:
        base_col_def = super().sql_col_def()
        if self._auto_increment:
            return f"{base_col_def} GENERATED ALWAYS AS IDENTITY"
        return base_col_def


class Short(IntegralField):
    """Field for Spark's ShortType."""

    @property
    def _spark_type_class(self) -> Type[ShortType]:
        return ShortType

    def sql_type(self) -> str:
        return "SMALLINT"


class Decimal(FractionalField):
    """Field for Spark's DecimalType."""

    DEFAULT_PRECISION = 10
    DEFAULT_SCALE = 0

    __precision: int
    __scale: int

    def __init__(self, precision: int = DEFAULT_PRECISION, scale: int = DEFAULT_SCALE, **kwargs: Any) -> None:
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

    @classmethod
    def from_spark_struct_field(cls, spark_struct_field: StructField, use_name: bool = False) -> "Decimal":
        nullable = spark_struct_field.nullable
        metadata = spark_struct_field.metadata
        partitioned_by = metadata.get(PARTITIONED_BY_KEY, False)
        name = spark_struct_field.name if use_name else None
        dtype = spark_struct_field.dataType
        assert isinstance(dtype, DecimalType)
        precision, scale = dtype.precision, dtype.scale
        return cls(precision=precision, scale=scale, nullable=nullable, name=name, metadata=metadata, partitioned_by=partitioned_by)

    def _get_args(self) -> Tuple[str, ...]:
        base_args = super()._get_args()
        precision_arg = f"precision={self.__precision}" if self.__precision != self.DEFAULT_PRECISION else None
        scale_arg = f"scale={self.__scale}" if self.__scale != self.DEFAULT_SCALE else None
        non_default_args = tuple(arg for arg in (precision_arg, scale_arg) if arg is not None)
        return non_default_args + base_args

    def sql_type(self) -> str:
        data_type = self._spark_data_type
        return f"DECIMAL({data_type.precision},{data_type.scale})"


class Double(FractionalField):
    """Field for Spark's DoubleType."""

    @property
    def _spark_type_class(self) -> Type[DoubleType]:
        return DoubleType

    def _validate_on_value(self, value: Any) -> None:
        super()._validate_on_value(value)
        _validate_value_type_for_field((float,), value)

    def sql_type(self) -> str:
        return "DOUBLE"


class Float(FractionalField):
    """Field for Spark's FloatType."""

    @property
    def _spark_type_class(self) -> Type[FloatType]:
        return FloatType

    def _validate_on_value(self, value: Any) -> None:
        super()._validate_on_value(value)
        _validate_value_type_for_field((float,), value)

    def sql_type(self) -> str:
        return "FLOAT"


class String(AtomicField):
    """Field for Spark's StringType."""

    @property
    def _spark_type_class(self) -> Type[StringType]:
        return StringType

    def _validate_on_value(self, value: Any) -> None:
        super()._validate_on_value(value)
        _validate_value_type_for_field((str,), value)

    def sql_type(self) -> str:
        return "STRING"


class Binary(AtomicField):
    """Field for Spark's BinaryType."""

    @property
    def _spark_type_class(self) -> Type[BinaryType]:
        return BinaryType

    def _validate_on_value(self, value: Any) -> None:
        super()._validate_on_value(value)
        _validate_value_type_for_field((bytearray,), value)

    def sql_type(self) -> str:
        return "BINARY"


class Boolean(AtomicField):
    """Field for Spark's BooleanType."""

    @property
    def _spark_type_class(self) -> Type[BooleanType]:
        return BooleanType

    def _validate_on_value(self, value: Any) -> None:
        super()._validate_on_value(value)
        _validate_value_type_for_field((bool,), value)

    def sql_type(self) -> str:
        return "BOOLEAN"


class Date(AtomicField):
    """Field for Spark's DateType."""

    @property
    def _spark_type_class(self) -> Type[DateType]:
        return DateType

    def _validate_on_value(self, value: Any) -> None:
        super()._validate_on_value(value)
        _validate_value_type_for_field((date, datetime), value)

    def sql_type(self) -> str:
        return "DATE"


class Timestamp(AtomicField):
    """Field for Spark's TimestampType."""

    @property
    def _spark_type_class(self) -> Type[TimestampType]:
        return TimestampType

    def _validate_on_value(self, value: Any) -> None:
        super()._validate_on_value(value)
        _validate_value_type_for_field((datetime,), value)

    def sql_type(self) -> str:
        return "TIMESTAMP"


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
        partitioned_by: bool = False,
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
            raise FieldValueValidationError(f"Value for an array must not be a string. Found value '{value}'. Did you mean to use a list of " "strings?")
        for item in value:
            element_field = self.e
            if not element_field._is_nullable and item is None:  # pylint: disable=protected-access
                # to improve readability for errors, we preemptively validate the non-nullability of the array
                # element here
                msg = "Encountered None value in array, but the element field of this array is specified as " "non-nullable"
                if self._resolve_field_name() is not None:
                    msg += f" (array field name = '{self._resolve_field_name()}')"
                raise FieldValueValidationError(msg)
            self.e._validate_on_value(item)  # pylint: disable=protected-access

    #
    # Misc

    def __eq__(self, other: Any) -> bool:
        """True if `self` equals `other`."""
        return super().__eq__(other) and isinstance(other, Array) and self.e == other.e

    def _get_args(self) -> Tuple[str, ...]:
        base_args = super()._get_args()
        element_arg = f"element={self.e!r}"
        non_default_args = tuple(arg for arg in (element_arg,) if arg is not None)
        return non_default_args + base_args

    @classmethod
    def from_spark_struct_field(cls, spark_struct_field: StructField, use_name: bool = False) -> "BaseField":
        element_type = spark_struct_field.dataType.elementType

        orm_type = SPARK_TO_ORM_TYPE[type(element_type)]
        element_struct_field = StructField(name=spark_struct_field.name, dataType=element_type, nullable=spark_struct_field.nullable)
        orm_field = orm_type.from_spark_struct_field(element_struct_field, use_name=False)

        nullable = spark_struct_field.nullable
        metadata = spark_struct_field.metadata
        name = spark_struct_field.name if use_name else None
        return cls(orm_field, nullable=nullable, name=name, metadata=metadata)

    def sql_type(self) -> str:
        element_sql_type = self.e.sql_type()
        return f"ARRAY<{element_sql_type}>"


KeyType = TypeVar("KeyType", bound=BaseField)  # pylint: disable=invalid-name
ValueType = TypeVar("ValueType", bound=BaseField)  # pylint: disable=invalid-name


class Map(Generic[KeyType, ValueType], BaseField):
    """
    Map field; shadows MapType in the Spark API.

    Attributes:
        k:
            Data type info for the key of this map. Should be an instance of a `BaseField`.
            Received in constructor as "key".
        v:
            Data type info for the value of this map. Should be an instance of a `BaseField`.
            Received in constructor as "value".
    """

    __hash__ = BaseField.__hash__

    k: KeyType  # pylint: disable=invalid-name
    v: ValueType  # pylint: disable=invalid-name

    def __init__(
        self,
        key: KeyType,
        value: ValueType,
        nullable: bool = True,
        name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(nullable=nullable, name=name, metadata=metadata)

        if not isinstance(key, BaseField):
            raise ValueError(f"Map key must be a field. Found type: {type(key).__name__}")

        if key._resolve_field_name() is not None:
            raise ValueError(
                f"When using a field as the key field of a map, the field should not have a name. " f"The field's name resolved to: {key._resolve_field_name()}"
            )

        self.k = cast(BaseField, key._replace_explicit_name(name=self._explicit_name))

        if not isinstance(value, BaseField):
            raise ValueError(f"Map key must be a field. Found type: {type(value).__name__}")

        if value._resolve_field_name() is not None:
            raise ValueError(
                f"When using a field as the key field of a map, the field should not have a name. "
                f"The field's name resolved to: {value._resolve_field_name()}"
            )

        self.v = cast(BaseField, value._replace_explicit_name(name=self._explicit_name))

    def _replace_parent(self, parent: Optional["Struct"] = None) -> "Map[KeyType, ValueType]":
        """Return a copy of this array with the parent attribute set."""
        self_copy = copy.copy(self)
        if self_copy._parent_struct is not None:  # pylint: disable=protected-access
            raise ValueError("Cannot set parent on a field that already has a parent")
        self_copy._parent_struct = parent  # pylint: disable=protected-access

        self_copy.k = cast(KeyType, self.k._replace_parent(parent=parent))  # pylint: disable=protected-access
        self_copy.v = cast(ValueType, self.v._replace_parent(parent=parent))  # pylint: disable=protected-access

        return self_copy

    def _set_contextual_name(self, value: str) -> None:
        super()._set_contextual_name(value)
        # set child to same name as parent; i.e., propagate contextual name downwards:
        self.k._set_contextual_name(value)  # pylint: disable=protected-access
        self.v._set_contextual_name(value)  # pylint: disable=protected-access

    @property
    def _spark_type_class(self) -> Type[MapType]:
        return MapType

    @property
    def _spark_struct_field(self) -> StructField:
        """The Spark StructField for this field."""
        return StructField(
            name=self._field_name,
            dataType=MapType(
                keyType=self.k._spark_struct_field.dataType,  # pylint: disable=protected-access
                valueType=self.v._spark_struct_field.dataType,  # pylint: disable=protected-access
                valueContainsNull=self.v._is_nullable,  # pylint: disable=protected-access
            ),
            nullable=self._is_nullable,
            metadata=self._metadata,
        )

    def _validate_on_value(self, value: Any) -> None:
        super()._validate_on_value(value)
        if value is None:
            return
        if not isinstance(value, dict):
            raise FieldValueValidationError(f"Value for a map must be a dictionary, not '{type(value).__name__}'")
        for k, v in value.items():
            self.k._validate_on_value(k)  # pylint: disable=protected-access
            self.v._validate_on_value(v)  # pylint: disable=protected-access

    def __eq__(self, other: Any) -> bool:
        """True if `self` equals `other`."""
        return super().__eq__(other) and isinstance(other, Map) and self.k == other.k and self.v == other.v

    def _get_args(self) -> Tuple[str, ...]:
        base_args = super()._get_args()
        key_arg = f"key={self.k!r}"
        value_arg = f"value={self.v!r}"
        non_default_args = tuple(arg for arg in (key_arg, value_arg) if arg is not None)
        return non_default_args + base_args

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

        return getattr(super().__getattribute__("v"), attr_name)

    @classmethod
    def from_spark_struct_field(cls, spark_struct_field: StructField, use_name: bool = False) -> "BaseField":
        keyType = spark_struct_field.dataType.keyType
        valueType = spark_struct_field.dataType.valueType

        orm_key_type = SPARK_TO_ORM_TYPE[type(keyType)]
        key_struct_field = StructField(name=spark_struct_field.name, dataType=keyType, nullable=spark_struct_field.nullable)
        orm_key_field = orm_key_type.from_spark_struct_field(key_struct_field, use_name=False)

        orm_value_type = SPARK_TO_ORM_TYPE[type(valueType)]
        value_struct_field = StructField(name=spark_struct_field.name, dataType=valueType, nullable=spark_struct_field.nullable)
        orm_value_field = orm_value_type.from_spark_struct_field(value_struct_field, use_name=False)

        nullable = spark_struct_field.nullable
        metadata = spark_struct_field.metadata
        # partitioned_by = metadata.get(PARTITIONED_BY_KEY, False)
        name = spark_struct_field.name if use_name else None
        return cls(orm_key_field, orm_value_field, nullable=nullable, name=name, metadata=metadata)

    def sql_type(self) -> str:
        key_type = self.k.sql_type()
        value_type = self.v.sql_type()
        return f"MAP<{key_type},{value_type}>"


SPARK_TO_ORM_TYPE = {
    StringType: String,
    BooleanType: Boolean,
    IntegerType: Integer,
    FloatType: Float,
    DoubleType: Double,
    DateType: Date,
    TimestampType: Timestamp,
    LongType: Long,
    ShortType: Short,
    ByteType: Byte,
    BinaryType: Binary,
    DecimalType: Decimal,
    ArrayType: Array,
    MapType: Map,
    StructType: Struct,
}
