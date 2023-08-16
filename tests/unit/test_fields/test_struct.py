import pytest

from sparkorm.struct import _Validator, _FieldsExtractor
from sparkorm.exceptions import InvalidStructError
from sparkorm import String
from sparkorm.struct import Struct


class MockStruct(Struct):
    const = "HELLO"


class TestStruct:
    @staticmethod
    def test_should_not_permit_field_overrides_of_internal_properties():
        # given, when, then
        with pytest.raises(InvalidStructError):

            class Article(Struct):
                _spark_type_class = String()

    @staticmethod
    def test_should_not_permit_field_overrides_of_reserved_properties():
        # given, when, then
        with pytest.raises(InvalidStructError):

            class Article(Struct):
                validate_data_frame = String()

    @staticmethod
    def test_should_not_permit_fields_starting_with_underscore():
        # given, when, then
        with pytest.raises(InvalidStructError):

            class Article(Struct):
                _name = String()

    @staticmethod
    def test_should_not_inner_meta_that_is_not_a_class():
        # given, when, then
        with pytest.raises(InvalidStructError):

            class Article(Struct):
                Meta = String()

    @staticmethod
    def test_should_allow_override_of_functions():
        # given, when, then
        class Article(Struct):
            @property
            def _spark_type_class(self):
                return None

    @staticmethod
    def test_should_handle_non_field_attribute_on_struct_instance():
        # given
        class Article(Struct):
            other_const = "HELLO"

        # when
        value = Article().other_const

        # then
        assert value == "HELLO"

    @staticmethod
    @pytest.mark.parametrize(
        "obj", [pytest.param(MockStruct(), id="as instance"), pytest.param(MockStruct, id="as class")]
    )
    def should_be_hashable(obj):
        _field_can_be_used_as_a_key = {obj: "value"}


class TestValidator:
    @staticmethod
    def should_reject_class_missing_metadata():
        # given
        class Empty(Struct):
            pass

        Empty._struct_metadata = None
        validator = _Validator(Empty)

        # when, then
        with pytest.raises(ValueError, match="has not had its inner metadata extracted"):
            validator.validate()

    @staticmethod
    def should_ensure_struct_class_inherits_from_struct():
        # given
        class Empty:
            pass

        # when, then
        with pytest.raises(ValueError, match="'struct_class' must inherit from Struct"):
            _Validator(Empty)

    @staticmethod
    def should_ensure_struct_class_is_not_struct():
        # given, when, then
        with pytest.raises(ValueError, match="'struct_class' must not be Struct"):
            _Validator(Struct)


class TestFieldExtractor:
    @staticmethod
    def should_ensure_struct_class_inherits_from_struct():
        # given
        class Empty:
            pass

        # when, then
        with pytest.raises(ValueError, match="'struct_class' must inherit from Struct"):
            _FieldsExtractor(Empty)

    @staticmethod
    def should_ensure_struct_class_is_not_struct():
        # given, when, then
        with pytest.raises(ValueError, match="'struct_class' must not be Struct"):
            _FieldsExtractor(Struct)
