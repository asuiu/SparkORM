import pytest
from pyspark.sql.types import StructField, MapType, StringType, ArrayType

from sparkorm import Float, String, Array, Map
from sparkorm.struct import Struct


class TestMapField:

    @staticmethod
    def test_should_not_allow_element_with_explicit_name():
        # given
        explicit_name = String(name="explicit_name")
        implicit_name = Float()

        # when, then
        with pytest.raises(ValueError):
            Map(explicit_name, implicit_name)

        with pytest.raises(ValueError):
            Map(implicit_name, explicit_name, )

    @staticmethod
    @pytest.mark.skip(reason="Undecided how to implement Path on Map")
    def test_should_enable_path_via_explicit_element_field():
        # given
        class ComplexElementStruct(Struct):
            string_field = String()
            float_field = Float()

        class OuterObject(Struct):
            sequence = Map(String(), ComplexElementStruct())

        # when
        path = OuterObject.sequence.v.string_field.PATH

        # then
        assert path == "sequence.string_field"

    @staticmethod
    @pytest.mark.skip(reason="Undecided how to implement Path on Map")
    def should_enable_path_via_passthrough():
        # given
        class ComplexElementStruct(Struct):
            string_field = String()
            float_field = Float()

        class OuterObject(Struct):
            sequence = Map(ComplexElementStruct())

        # when
        path = OuterObject.sequence.string_field.PATH

        # then
        assert path == "sequence.string_field"

    @staticmethod
    @pytest.mark.skip(reason="Undecided how to implement Path on Map")
    def should_replace_parent_should_replace_parent_of_element():
        # given
        array_element = String()
        array = Map(array_element, name="array")

        class ParentStruct(Struct):
            pass

        new_parent = ParentStruct()

        # when
        returned_array = array._replace_parent(new_parent)

        # then
        assert array._parent_struct is None
        assert returned_array is not array
        assert returned_array._parent_struct is new_parent
        assert isinstance(returned_array, Map)
        assert returned_array.e._parent_struct is new_parent

    @staticmethod
    def test_should_reject_non_field_element():
        bad_element = "this is a str, which is not a field"

        with pytest.raises(ValueError):
            Map(bad_element, Float())
        with pytest.raises(ValueError):
            Map(Float, bad_element)

    @staticmethod
    @pytest.mark.parametrize(
        "instance",
        [
            pytest.param(Map(Float(), Float(), True, "name"), id="nullable instance"),
            pytest.param(Map(Float(), Float(), False, "name"), id="non-nullable instance"),
            pytest.param(Map(Float(), Float(), False), id="non-nullable nameless instance"),
            pytest.param(Map(Float(), Float(), ), id="instance with default constructor"),
        ],
    )
    def test_should_be_hashable(instance: Map):
        _field_can_be_used_as_a_key = {instance: "value"}

    @staticmethod
    def test_repr_nominal():
        element1 = String(nullable=False)
        element2 = Float(nullable=False)
        element1_val = repr(element1)
        element2_val = repr(element2)
        field = Map(element1, element2)
        assert repr(field) == f"Map(key={element1_val}, value={element2_val})"
        field = Map(element1, element2, nullable=False)
        assert repr(field) == f"Map(key={element1_val}, value={element2_val}, nullable=False)"

        field = Map(element1, element2, name="test_name")
        expected_element1_val = f"String(nullable=False, name='test_name')"
        expected_element2_val = f"Float(nullable=False, name='test_name')"
        assert repr(field) == f"Map(key={expected_element1_val}, value={expected_element2_val}, name='test_name')"

        field = Map(element1, element2, name="test_name", metadata={"a": "b"}, nullable=False)
        assert repr(field) == f"Map(key={expected_element1_val}, value={expected_element2_val}, nullable=False, name='test_name', metadata={{'a': 'b'}})"

    @staticmethod
    def test_from_spark_struct_field_with_array_of_strings():
        struct_field = StructField('map_field', MapType(StringType(), ArrayType(ArrayType(StringType(), True), True), True), True)
        orm_field = Map.from_spark_struct_field(struct_field)
        assert isinstance(orm_field, Map)
        assert isinstance(orm_field.k, String)
        assert isinstance(orm_field.v, Array)
        assert isinstance(orm_field.v.e, Array)
        assert isinstance(orm_field.v.e.e, String)

