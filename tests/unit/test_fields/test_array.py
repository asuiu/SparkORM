import re

import pytest
from pyspark.sql.types import StringType, ArrayType, StructField

from sparkorm import String, Array, Float
from sparkorm.struct import Struct


class TestArrayField:
    @staticmethod
    def should_not_allow_element_with_explicit_name():
        # given
        element = String(name="explicit_name")

        # when, then
        with pytest.raises(
            ValueError, match="When using a field as the element field of an array, the field should not have a name."
        ):
            Array(element)

    @staticmethod
    def should_enable_path_via_explicit_element_field():
        # given
        class ComplexElementStruct(Struct):
            string_field = String()
            float_field = Float()

        class OuterObject(Struct):
            sequence = Array(ComplexElementStruct())

        # when
        path = OuterObject.sequence.e.string_field.PATH

        # then
        assert path == "sequence.string_field"

    @staticmethod
    def should_enable_path_via_passthrough():
        # given
        class ComplexElementStruct(Struct):
            string_field = String()
            float_field = Float()

        class OuterObject(Struct):
            sequence = Array(ComplexElementStruct())

        # when
        path = OuterObject.sequence.string_field.PATH

        # then
        assert path == "sequence.string_field"

    @staticmethod
    def should_replace_parent_should_replace_parent_of_element():
        # given
        array_element = String()
        array = Array(array_element, name="array")

        class ParentStruct(Struct):
            pass

        new_parent = ParentStruct()

        # when
        returned_array = array._replace_parent(new_parent)

        # then
        assert array._parent_struct is None
        assert returned_array is not array
        assert returned_array._parent_struct is new_parent
        assert isinstance(returned_array, Array)
        assert returned_array.e._parent_struct is new_parent

    @staticmethod
    def should_reject_non_field_element():
        # given
        bad_element = "this is a str, which is not a field"

        # when, then
        with pytest.raises(ValueError, match=re.escape("Array element must be a field. Found type: str")):
            Array(bad_element)

    @staticmethod
    @pytest.mark.parametrize(
        "instance",
        [
            pytest.param(Array(Float(), True, "name"), id="nullable instance"),
            pytest.param(Array(Float(), False, "name"), id="non-nullable instance"),
            pytest.param(Array(Float(), False), id="non-nullable nameless instance"),
            pytest.param(Array(Float()), id="instance with default constructor"),
        ],
    )
    def should_be_hashable(instance: Array):
        _field_can_be_used_as_a_key = {instance: "value"}

    @staticmethod
    def test_repr_nominal():
        element = String(nullable=False)
        element_val = repr(element)
        field = Array(element)
        assert repr(field) == f"Array(element={element_val})"
        field = Array(element, nullable=False)
        assert repr(field) == f"Array(element={element_val}, nullable=False)"

        field = Array(element, name="test_name")
        expected_element_val = f"String(nullable=False, name='test_name')"
        assert repr(field) == f"Array(element={expected_element_val}, name='test_name')"

        field = Array(element, name="test_name", metadata={"a": "b"}, nullable=False)
        assert repr(field) == f"Array(element={expected_element_val}, nullable=False, name='test_name', metadata={{'a': 'b'}})"

    @staticmethod
    def test_from_spark_struct_field_with_array_of_strings():
        array_struct_field = StructField('array', ArrayType(ArrayType(StringType(), True), True))
        orm_field = Array.from_spark_struct_field(array_struct_field)
        assert isinstance(orm_field, Array)
        assert isinstance(orm_field.e, Array)
        assert isinstance(orm_field.e.e, String)

