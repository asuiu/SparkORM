"""Struct."""

import dataclasses
from collections import OrderedDict
from dataclasses import dataclass
from difflib import ndiff
from inspect import isclass
from typing import ClassVar, Optional, Mapping, Type, Any, Generator, Tuple, MutableMapping, Dict, List

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, Row

from sparkorm.formatters import pretty_schema
from sparkorm.schema_builder import schema as schematise
from sparkorm.exceptions import (
    InvalidStructError,
    StructImplementationError,
    InvalidDataFrameError,
    StructInstantiationArgumentsError,
    StructInstantiationArgumentTypeError,
    FieldValueValidationError,
)
from sparkorm.fields.base import BaseField


@dataclass(frozen=True)
class ValidationResult:
    """
    Describes the result of attempting to validate a struct on a DataFrame's schema.

    Attributes:
        is_valid: Is the DataFrame valid?
        pretty_struct: Human-readable Spark schema for the Struct.
        pretty_data_frame: Human-readable Spark schema for the DataFrame.
        report: A human-readable report on the differences between the two frames. Empty string if no differences.
    """

    is_valid: bool
    pretty_struct: str
    pretty_data_frame: str
    report: str = ""

    def raise_on_invalid(self) -> None:
        """
        If the DataFrame did not adhere to the schema, raises an appropriate error.

        Raises:
            InvalidDataFrameError if the DataFrame does not pass validation.
        """
        if not self.is_valid:
            raise InvalidDataFrameError(self.report)


class Struct(BaseField):
    """A struct; shadows StructType in the Spark API."""

    __hash__ = BaseField.__hash__
    _struct_metadata: ClassVar[Optional["_StructInnerMetadata"]] = None

    @classmethod
    def _valid_struct_metadata(cls) -> "_StructInnerMetadata":
        """
        A wrapper for the `_struct_metadata` class variable, which ensures a non-None return, failing if None.

        Raises:
            ValueError:
                If the class's `_struct_metadata` is None, which means its inner metadata has not been set. The
                noneness of `_struct_metadata` is managed by sparkorm under-the-hood. In normal use of sparkorm,
                this should never occur.
        """
        # using this method instead of `_struct_metadata` (in appropriate contexts) simplifies type hinting by ensuring
        # other functions are always dealing with a non-null `_struct_metadata`
        if cls._struct_metadata is None:
            raise ValueError(f"Struct class {cls.__name__} has not had its inner metadata extracted")
        return cls._struct_metadata

    @classmethod
    def validate_data_frame(cls, dframe: DataFrame) -> ValidationResult:
        """
        Validate that the DataFrame `dframe` adheres to the schema of this struct.

        Args:
            dframe: DataFrame to be validated.

        Returns:
            DataFrameValidationSummary, which describes whether the DataFrame is valid, and additional information
            on the differences.
        """
        schematised_struct = schematise(cls)

        is_valid = dframe.schema == schematised_struct
        pretty_struct = pretty_schema(schematised_struct)
        pretty_dframe = pretty_schema(dframe.schema)
        if is_valid:
            return ValidationResult(is_valid, pretty_struct=pretty_struct, pretty_data_frame=pretty_dframe)

        report = f"Struct schema...\n\n{pretty_struct}\n\n"
        report += f"DataFrame schema...\n\n{pretty_dframe}\n\n"
        report += "Diff of struct -> data frame...\n\n"
        report += "\n".join(
            line_diff
            for line_diff in ndiff(pretty_dframe.splitlines(), pretty_struct.splitlines())
            if not line_diff.startswith("?")
        )
        return ValidationResult(is_valid, pretty_struct=pretty_struct, pretty_data_frame=pretty_dframe, report=report)

    #
    # Handle type management and Spark representations for a Struct object

    @property
    def _spark_type_class(self) -> Type[StructType]:
        return StructType

    @property
    def _spark_struct_field(self) -> StructField:
        """The Spark StructField for this field."""
        return StructField(
            name=self._field_name,
            dataType=self._valid_struct_metadata().spark_struct,
            nullable=self._is_nullable,
            metadata=self._metadata,
        )

    def _validate_on_value(self, value: Any) -> None:
        super()._validate_on_value(value)
        if value is None:
            # super() will have already validate none vs nullability. if None, then it's safe to be none
            return
        if not isinstance(value, Mapping):
            raise FieldValueValidationError(f"Value for a struct must be a mapping, not '{type(value).__name__}'")

        dic: Mapping[str, Any] = value

        fields = list(self._valid_struct_metadata().fields.values())
        field_names = [field._field_name for field in fields]  # pylint: disable=protected-access
        if len(fields) != len(dic):
            raise FieldValueValidationError(
                f"Dict has incorrect number of fields. \n"
                f"Struct requires {len(fields)} fields: {', '.join(field_names)} \n"
                f"Dict has {len(dic)} fields: {', '.join(dic.keys())}"
            )

        if field_names != list(dic.keys()):
            raise FieldValueValidationError(
                f"Dict fields do not match struct fields. \n"
                f"Struct fields: {', '.join(field_names)} \n"
                f"Dict fields: {', '.join(dic.keys())}"
            )

        for field in fields:
            field._validate_on_value(dic[field._field_name])  # pylint: disable=protected-access

    #
    # Hook in to sub-class creation. Ensure fields are pre-processed when a sub-class is declared

    @classmethod
    def __init_subclass__(cls, **options: Any) -> None:  # pylint: disable=unused-argument
        """Hook in to the subclassing of this base class; process fields when sub-classing occurs."""
        super().__init_subclass__(**options)

        # Ensure a subclass does not break any base class functionality
        for child_prop, child_val in cls.__dict__.items():
            if (child_prop in Struct.__dict__) and (isinstance(child_val, BaseField)):
                raise InvalidStructError(
                    f"Field should not override inherited or reserved class properties: {child_prop}"
                )

        # Extract internal metadata for this class, including parsing the fields
        cls._struct_metadata = _StructInnerMetadata.from_struct_class(cls)

        # Validate the "implements" (if any)
        _Validator(cls).validate()

    #
    # Handle dot chaining for full path ref to nested fields

    def __getattribute__(self, attr_name: str) -> Any:
        """
        Customise how field attributes are handled.

        Augment the attribute reference chain to ensure that a field's parent is set.
        """
        attr_value = super().__getattribute__(attr_name)

        if attr_name in {"_struct_metadata", "_valid_struct_metadata"}:
            return attr_value

        resolved_field = self._valid_struct_metadata().resolve_field(
            struct_object=self, attr_name=attr_name, attr_value=attr_value
        )
        if resolved_field is not None:
            return resolved_field

        return attr_value

    #
    # Makers

    @classmethod
    def make_dict(cls, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        """
        Create a data instance of this Struct schema, as a dictionary.

        Specify the values for the fields of this struct using args and keyword args, in the same way you would
        specify values in a constructor. Use the struct's property names are keyword arguments. Omitted fields default
        to null.

        Values are validated according to the struct's schema. Null values are only permitted in nullable fields.

        For example, given a struct:

            ```
            class Article(Struct):
                title = String(name="long_title")
                body = String()
            ```

        then we can write:

            ```
            Article.make_dict(
                title=The title",
                body="The body of the article."
            )
            ```

        to create a dictionary:

            ```
            { "long_title": "The title", "body": "The body of the article."}
            ```

        Raises:
            StructInstantiationArgumentsError:
                If arguments are not specified correctly.
            StructInstantiationArgumentTypeError:
                If a value does not match a field's expected type. Or if attempting to use a null in a non-nullable
                field.

        Returns:
            An instance of this struct, as a dictioniary.
        """
        return _DictMaker(struct_class=cls, positional_args=args, keyword_args=kwargs).make_dict()

    @classmethod
    def make_row(cls, *args: Any, **kwargs: Any) -> Row:
        """Reserved."""
        # pylint: disable=broad-exception-raised
        raise Exception("Function name reserved.")

    #
    # Other methods

    def __eq__(self, other: Any) -> bool:
        """True if `self` equals `other`."""
        return (
            super().__eq__(other)
            and isinstance(other, Struct)
            and self._valid_struct_metadata().fields == other._valid_struct_metadata().fields
            and list(self._valid_struct_metadata().fields.keys()) == list(other._valid_struct_metadata().fields.keys())
        )


@dataclass(frozen=True)
class _StructInnerMetadata:
    """Inner metadata object for Struct classes; hides complexity of field handling."""

    fields: Mapping[str, BaseField]
    # ^ All fields for the Struct, including both native fields and imported from includes

    @staticmethod
    def from_struct_class(struct_class: Type[Struct]) -> "_StructInnerMetadata":
        """Build instance from a struct class."""
        return _StructInnerMetadata(fields=_FieldsExtractor(struct_class).extract())

    @property
    def spark_struct(self) -> StructType:
        """Complete Spark StructType for the sparkorm Struct."""
        return StructType(
            [field._spark_struct_field for field in self.fields.values()]  # pylint: disable=protected-access
        )

    # Resolving of class attributes
    @staticmethod
    def resolve_field(struct_object: Struct, attr_name: str, attr_value: Any) -> Optional[BaseField]:
        """
        Attempt to resolve a `getattribute` call on the Struct, returning a BaseField if applicable.

        This should be used to hook into the Struct's `getattribute` behaviour to customise resolution of
        fields. Will also check if the `getattribute` call is attempting to resolve a field. If not, returns None.

        Args:
            struct_object: The instance of the Struct upon which `getattribute` was called.
            attr_name: The name of the `struct_object` attribute to be resolved.
            attr_value: The attribute of the `struct_object`.
        """
        if attr_name.startswith("_"):
            return None

        if isinstance(attr_value, BaseField):
            new_field: BaseField = attr_value._replace_parent(parent=struct_object)  # pylint: disable=protected-access
            return new_field

        return None


#
# Supporting functionality
_META_INNER_CLASS_NAME = "Meta"


@dataclass
class _FieldsExtractor:
    """
    Extracts a Struct's fields from its class, also handling inner Meta, super class, and so on.

    Definitions...

    - Root Struct: The sub-class of Struct being managed by this Handler.
    - Native fields: Fields explicitly declared in the Struct.
    - Meta class: The inner class that can be (optionally) specified within a Struct sub-class. Found at
      `Struct.Meta`.
    - Include Struct: One of (possibly many) Structs specified in the root struct's `Meta.includes`.
    - Includes fields: Fields pulled in from `Meta.includes`.
    """

    INCLUDES_FIELD_NAME: ClassVar[str] = "includes"

    struct_class: Type[Struct]

    def __post_init__(self) -> None:
        """Validate input instance variables."""
        _validate_struct_class(self.struct_class)

    def extract(self) -> Mapping[str, BaseField]:
        """Extract the fields."""
        # pylint: disable=attribute-defined-outside-init

        # Extract and hand native fields
        # native field name -> field
        self._native_fields = OrderedDict(self._yield_native_fields())
        for field_name, field in self._native_fields.items():
            field._set_contextual_name(field_name)  # pylint: disable=protected-access

        # Start with super class fields, if any
        self._fields: MutableMapping[str, BaseField] = OrderedDict(self._get_super_class_fields())

        # Add native fields
        self._fields = self._safe_combine_fields(self._fields, self._native_fields)

        # Add includes fields (if any)
        for included_struct in self._yield_included_structs():
            incl_fields = included_struct._valid_struct_metadata().fields  # pylint: disable=protected-access
            for incl_field_name, incl_field in incl_fields.items():
                if incl_field_name not in self._fields:
                    self._fields[incl_field_name] = incl_field  # populate `_fields`
                    # Modify the root struct so that includes fields are available:
                    setattr(self.struct_class, incl_field_name, incl_field)
                elif self._fields[incl_field_name] != incl_field:
                    raise InvalidStructError(
                        "Attempting to replace a field with an 'includes' field of different type. "
                        f"From includes class: {type(included_struct)}. Incompatible field name: {incl_field_name}"
                    )

        return self._fields

    #
    # Extraction and processing of the Struct class

    def _yield_native_fields(self) -> Generator[Tuple[str, BaseField], None, None]:
        """
        Get the native fields specified for this Struct class, if any.

        Yields:
            A `(str, BaseField)` pair for each field found in this class. Each pair consists of the attribute name and
            the field.
        """
        for attr_name, attr_value in self.struct_class.__dict__.items():
            if not isinstance(attr_value, BaseField):
                continue
            if attr_name.startswith("_"):
                raise InvalidStructError(
                    f"Fields must not begin with an underscore. Found: {attr_name} = {type(attr_value)}"
                )
            yield (attr_name, attr_value)

    def _yield_included_structs(self) -> Generator[Struct, None, None]:
        """
        Get the Structs specified in the `Meta.includes`, if any.

        A class specified in `Meta.includes` is converted to an instance of the class before returning.

        Yields:
            A Struct object. Note that this is an instance of the Struct, not the class.
            If `Meta` or `Meta.includes` are not provided, no yield.
        """
        for struct in _yield_structs_from_meta(self.struct_class, self.INCLUDES_FIELD_NAME):
            yield struct

    #
    # Handle inheritance from super class

    def _get_super_class(self) -> Type[Struct]:
        """Obtain super class; this will be the Struct class, or a class that is a subclass (/descendant) of Struct."""
        root_class = self.struct_class

        assert len(root_class.__mro__) >= 2
        # by definition, the `_FieldsExtractor` will only be invoked when the `root_class` is subclassing Struct, and
        # thus the MRO for the `root_class` will always have at least two elems

        return root_class.__mro__[1]

    def _get_super_struct_metadata(self) -> Optional[_StructInnerMetadata]:
        """Obtain super class's inner metadata; or None if none found."""
        super_class = self._get_super_class()
        assert super_class is not None and hasattr(super_class, "_struct_metadata")

        if super_class is Struct:
            # we know that the Struct class (i.e., the base for all custom Structs)
            return None

        super_struct_metadata = getattr(super_class, "_struct_metadata")
        assert isinstance(super_struct_metadata, _StructInnerMetadata)
        return super_struct_metadata

    def _get_super_class_fields(self) -> Mapping[str, BaseField]:
        """Returns fields (mapping of attr_name -> field) from the super class; or empty if none found."""
        super_metadata = self._get_super_struct_metadata()
        if super_metadata is None:
            return OrderedDict()
        return OrderedDict(super_metadata.fields)

    #
    # Helpers

    @staticmethod
    def _safe_combine_fields(
        fields_a: Mapping[str, BaseField], fields_b: Mapping[str, BaseField]
    ) -> MutableMapping[str, BaseField]:
        """
        Add fields from `fields_b` into `fields_a`, returning a new copy.

        Only new fields are added. Checks that duplicate fields are identical.
        """
        combined = OrderedDict(fields_a)
        for key, value_b in fields_b.items():
            if key not in combined:
                combined[key] = value_b
            elif combined[key] != value_b:
                raise InvalidStructError(
                    f"Attempting to replace field '{key}' with field of different type: "
                    f"{type(fields_a[key])} vs {type(fields_b[key])}"
                )
        return combined


@dataclass
class _Validator:
    """
    Validate the "implements" requirement of a struct, if any provided.

    Assumes that the inner metadata of the class has been extracted.
    """

    IMPLEMENTS_FIELD_NAME: ClassVar[str] = "implements"

    struct_class: Type[Struct]

    def __post_init__(self) -> None:
        """Validate input instance variables."""
        _validate_struct_class(self.struct_class)

    def validate(self) -> None:
        """
        Validate that the meets "implements" requirements, if specified.

        Raises:
            StructImplementationError:
                If class does not correctly implement any required structs.
        """
        root_struct_metadata: _StructInnerMetadata = (
            self.struct_class._valid_struct_metadata()  # pylint: disable=protected-access
        )

        for required_struct in self._yield_implements_structs():
            req_fields = required_struct._valid_struct_metadata().fields  # pylint: disable=protected-access
            for req_field_name, req_field in req_fields.items():
                if req_field_name not in root_struct_metadata.fields:
                    raise StructImplementationError(
                        f"Struct '{self.struct_class.__name__}' does not implement field '{req_field_name}' "
                        f"required by struct '{type(required_struct).__name__}'"
                    )
                if req_field != root_struct_metadata.fields[req_field_name]:
                    # pylint: disable=protected-access
                    raise StructImplementationError(
                        f"Struct '{self.struct_class.__name__}' "
                        f"implements field '{req_field_name}' "
                        f"(required by struct '{type(required_struct).__name__}') but field is not compatible. "
                        f"Required {req_field._short_info()} "
                        f"but found {root_struct_metadata.fields[req_field_name]._short_info()}"
                    )

    def _yield_implements_structs(self) -> Generator[Struct, None, None]:
        """Get the Structs specified in the `Meta.implements`, if any."""
        for struct in _yield_structs_from_meta(self.struct_class, self.IMPLEMENTS_FIELD_NAME):
            yield struct


def _get_inner_meta_class(struct_class: Type[Struct]) -> Optional[Type[Any]]:
    """Retrieve the `Meta` inner class of a Struct class, or None if none is provided."""
    if not hasattr(struct_class, _META_INNER_CLASS_NAME):
        return None
    inner_meta_class = getattr(struct_class, _META_INNER_CLASS_NAME)

    if not isinstance(inner_meta_class, type):
        raise InvalidStructError(
            f"The '{_META_INNER_CLASS_NAME}' "
            "property of a Struct must only be used as an "
            f"inner class. Found type {type(inner_meta_class)}"
        )
    return inner_meta_class


def _yield_structs_from_meta(source_struct_class: Type[Struct], attribute_name: str) -> Generator[Struct, None, None]:
    """
    Get a list of structs located at an attribute of the Meta inner class, if any.

    A class specified in the list of Structs is converted to an instance of the class before returning.

    Yields:
        A Struct object. Note that this is an instance of the Struct, not the class.
        If `Meta` or the attribute `attribute_name` of `Meta` are not present, no yield.
    """
    inner_meta_class = _get_inner_meta_class(source_struct_class)
    if inner_meta_class is None:
        return

    struct_classes = getattr(inner_meta_class, attribute_name, None)
    if struct_classes is None:
        return

    for index, struct_class in enumerate(struct_classes):
        if not isclass(struct_class):
            raise InvalidStructError(
                f"Encountered non-class item in '{attribute_name}' list of 'Meta' inner class. "
                f"Item at index {index}: '{struct_class}' with type {type(struct_class)}"
            )

        struct_instance = struct_class()
        if not isinstance(struct_instance, Struct):
            raise InvalidStructError(
                "Encountered item in 'includes' list of 'Meta' inner class that is not a Struct or Struct "
                f"subclass. Item at index {index} is {struct_class}"
            )

        yield struct_instance


def _validate_struct_class(struct_class: Type[Struct]) -> None:
    if not issubclass(struct_class, Struct):
        raise ValueError("'struct_class' must inherit from Struct")
    if struct_class is Struct:
        raise ValueError("'struct_class' must not be Struct")


@dataclass
class _DictMaker:
    """Construct an instance of a Struct, as a dictionary."""

    struct_class: Type[Struct]
    positional_args: Tuple[Any, ...]
    keyword_args: Dict[str, Any]

    # internal state
    _struct_property_to_field: Mapping[str, BaseField] = dataclasses.field(init=False)

    _property_to_value: Dict[str, List[Any]] = dataclasses.field(init=False)
    # ^ store extracted values while we process them. maps property name (i.e., attrib name) to a value.
    #   we'll track if there are any surplus values for the same field

    _surplus_positional_values: List[Any] = dataclasses.field(default_factory=list, init=False)
    _surplus_keyword_args: Dict[str, Any] = dataclasses.field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        # extract `_struct_metadata.fields` for convenience
        inner_meta = self.struct_class._valid_struct_metadata()  # pylint: disable=protected-access
        self._struct_property_to_field = inner_meta.fields
        self._property_to_value = OrderedDict((property_name, []) for property_name in self._struct_property_to_field)

    def _process_positional_args(self) -> None:
        property_to_field = OrderedDict(self._struct_property_to_field)
        args = list(self.positional_args)
        while args:
            arg_value = args.pop(0)
            # determine what field this value is for
            if not property_to_field:
                # we've run out of struct fields. we have surplus positional args
                self._surplus_positional_values.append(arg_value)
            else:
                property_name = next(iter(property_to_field.keys()))
                property_to_field.pop(property_name)
                self._property_to_value[property_name].append(arg_value)

    def _process_keyword_args(self) -> None:
        property_to_field = OrderedDict(self._struct_property_to_field)
        kwargs = dict(self.keyword_args)
        while kwargs:
            arg_property_name = next(iter(kwargs.keys()))
            arg_value = kwargs.pop(arg_property_name)

            if arg_property_name not in property_to_field:
                self._surplus_keyword_args[arg_property_name] = arg_value
            else:
                self._property_to_value[arg_property_name].append(arg_value)

    def make_dict(self) -> Dict[str, Any]:
        """Make a dictionary from the given args (see ivars)."""
        # note: "field name" is the concrete field name
        #       "property name" is the class attribute name
        self._process_positional_args()
        self._process_keyword_args()

        #
        # Identify unfilled props and duplicate (repeated) props
        # Note: unfilled_props are not error cases. they will be defaulted to null.
        # duplicate_props *are* error cases
        unfilled_props = []
        duplicate_props = []
        for property_name, values_list in self._property_to_value.items():
            if not values_list:
                unfilled_props.append(property_name)
            elif len(values_list) >= 2:
                duplicate_props.append(property_name)

        #
        # Default unfilled props to null
        for property_name, values_list in self._property_to_value.items():
            if not values_list:
                values_list.append(None)

        #
        # Apply arg handling validation
        if duplicate_props or self._surplus_positional_values or self._surplus_keyword_args:
            raise StructInstantiationArgumentsError(
                properties=list(self._struct_property_to_field.keys()),
                unfilled_properties=unfilled_props,
                duplicate_properties=duplicate_props,
                surplus_positional_values=self._surplus_positional_values,
                surplus_keyword_args=list(self._surplus_keyword_args.keys()),
            )

        #
        # Create instance, with correct ordering, and field name keys
        field_name_to_value = OrderedDict()
        for property_name, field in self._struct_property_to_field.items():
            values = self._property_to_value[property_name]
            assert len(values) == 1, values
            field_name_to_value[field._field_name] = values[0]  # pylint: disable=protected-access

        #
        # Validate arg types
        for field in self._struct_property_to_field.values():
            field_name = field._field_name  # pylint: disable=protected-access
            value = field_name_to_value[field_name]
            try:
                field._validate_on_value(value)  # pylint: disable=protected-access
            except FieldValueValidationError as ex:
                raise StructInstantiationArgumentTypeError(str(ex)) from ex

        return dict(field_name_to_value)
