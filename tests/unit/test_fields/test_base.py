from typing import Type, Any

import pytest
from pyspark.sql.types import StructField, DataType

from sparkorm import Float, String
from sparkorm.struct import Struct
from sparkorm.exceptions import FieldParentError, FieldNameError
from sparkorm.base_field import _pretty_path, BaseField


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
            pytest.param(Float(True, "name"), "Float(name='name')", id="nullable instance"),
            pytest.param(Float(False, "name"), "Float(nullable=False, name='name')", id="non-nullable instance"),
            pytest.param(Float(False), "Float(nullable=False)", id="non-nullable nameless instance"),
            pytest.param(Float(), "Float()", id="instance with default constructor"),
            pytest.param(
                Float(metadata={}),
                "Float()",
                id="instance with empty metadata",
            ),
            pytest.param(
                Float(metadata={"a": "b"}),
                "Float(metadata={'a': 'b'})",
                id="instance with one metadata item",
            ),
            pytest.param(
                Float(metadata={"a": "b", "x": "y"}),
                "Float(metadata={'a': 'b', 'x': 'y'})",
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

    @staticmethod
    def test_repr_nominal():
        class TestField(BaseField):
            """ implemetation of abstract methods: __eq__, _spark_struct_field, _spark_type_class, _validate_on_value """

            @property
            def _spark_type_class(self) -> Type[DataType]:
                raise NotImplementedError()

            def _validate_on_value(self, value: Any) -> None:
                raise NotImplementedError()

            @property
            def _spark_struct_field(self) -> StructField:
                raise NotImplementedError()

            def __eq__(self, other: Any) -> bool:
                raise NotImplementedError()

        field = TestField()
        assert repr(field) == "TestField()"
        field = TestField(nullable=False)
        assert repr(field) == "TestField(nullable=False)"
        field = TestField(name="test_name")
        assert repr(field) == "TestField(name='test_name')"
        field = TestField(partitioned_by=True)
        assert repr(field) == "TestField(partitioned_by=True)"
        field = TestField(metadata={"a": "b"})
        assert repr(field) == "TestField(metadata={'a': 'b'})"

        field = TestField(name="test_name", metadata={"a": "b"}, nullable=False, partitioned_by=True)
        assert repr(field) == "TestField(nullable=False, name='test_name', metadata={'a': 'b'}, partitioned_by=True)"


class TestPrettyPath:
    @staticmethod
    def should_prettify_a_path():
        # given (and above)
        seq = [String(name="field_a"), Float(name="field_b")]

        # when
        pretty_path_str = _pretty_path(seq)

        # then
        assert pretty_path_str == "< 'field_a' (String) -> 'field_b' (Float) >"
