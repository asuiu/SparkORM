import pytest

from sparkorm.fields.base import _pretty_path
from sparkorm.exceptions import FieldParentError, FieldNameError
from sparkorm import Float, Struct, String


@pytest.fixture()
def float_field() -> Float:
    return Float()


class TestBaseField:
    @staticmethod
    def should_give_correct_info_string(float_field: Float):
        assert (
            float_field._info() == "<Float\n"
            "  spark type = FloatType\n"
            "  nullable = True\n"
            "  name = None <- [None, None]\n"
            "  parent = None\n"
            "  metadata = {}\n"
            ">"
        )

    @staticmethod
    @pytest.mark.parametrize(
        "instance,expected_str",
        [
            pytest.param(Float(name="name"), "name", id="named instance"),
            pytest.param(Float(), "", id="nameless instance with default constructor"),
        ],
    )
    def should_have_a_string_representation_for(instance, expected_str):
        assert str(instance) == expected_str

    @staticmethod
    @pytest.mark.parametrize(
        "instance",
        [
            pytest.param(Float(True, "name"), id="nullable instance"),
            pytest.param(Float(False, "name"), id="non-nullable instance"),
            pytest.param(Float(False), id="non-nullable nameless instance"),
            pytest.param(Float(), id="instance with default constructor"),
        ],
    )
    def should_be_hashable(instance: Float):
        _field_can_be_used_as_a_key = {instance: "value"}

    @staticmethod
    @pytest.mark.parametrize(
        "instance,expected_repr",
        [
            pytest.param(Float(True, "name"), "<Nullable Float: name>", id="nullable instance"),
            pytest.param(Float(False, "name"), "<Float: name>", id="non-nullable instance"),
            pytest.param(Float(False), "<Float: None>", id="non-nullable nameless instance"),
            pytest.param(Float(), "<Nullable Float: None>", id="instance with default constructor"),
            pytest.param(
                Float(metadata={}),
                "<Nullable Float: None>",
                id="instance with empty metadata",
            ),
            pytest.param(
                Float(metadata={"a": "b"}),
                "<Nullable Float [with 1 metadata item]: None>",
                id="instance with one metadata item",
            ),
            pytest.param(
                Float(metadata={"a": "b", "x": "y"}),
                "<Nullable Float [with 2 metadata items]: None>",
                id="instance with two metadata items",
            ),
        ],
    )
    def should_have_a_readable_repr_for(instance, expected_repr):
        assert repr(instance) == expected_repr

    @staticmethod
    def should_reject_setting_a_set_parent():
        # given
        struct = Struct()
        float_field = Float()._replace_parent(struct)

        another_struct = Struct()

        # when, then
        with pytest.raises(FieldParentError):
            float_field._replace_parent(another_struct)

    @staticmethod
    def should_get_contextual_field_name(float_field: Float):
        # given
        float_field._set_contextual_name("contextual_name")

        # when
        contextual_name = float_field._contextual_name

        # then
        assert contextual_name == "contextual_name"

    @staticmethod
    def should_reject_overriding_a_set_contextual_name(float_field: Float):
        # given
        float_field._set_contextual_name("contextual_name")

        # when, then
        with pytest.raises(FieldNameError):
            float_field._set_contextual_name("another_name")

    @staticmethod
    def test_field_name_should_raise_error_if_not_resolved(float_field: Float):
        with pytest.raises(FieldNameError):
            float_field._field_name

    @staticmethod
    def test_should_reject_replacing_a_preexisting_explicit_name():
        # given
        float_field = Float(name="explicit_name")

        # wheb, then
        with pytest.raises(FieldNameError):
            float_field._replace_explicit_name("new_explicit_name")


class TestPrettyPath:
    @staticmethod
    def should_prettify_a_path():
        # given (and above)
        seq = [String(name="field_a"), Float(name="field_b")]

        # when
        pretty_path_str = _pretty_path(seq)

        # then
        assert pretty_path_str == "< 'field_a' (String) -> 'field_b' (Float) >"
