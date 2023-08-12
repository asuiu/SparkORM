import pytest

from sparkorm import Struct, String, Array, Float


class TestEquality:
    #
    # Atomics

    @staticmethod
    def test_two_atomics_are_equal():
        field = String()
        another_field = String()
        assert field == another_field

    #
    # Structs

    @staticmethod
    def test_two_structs_are_equal():
        # given
        class ObjectA(Struct):
            string_field = String()

        class ObjectB(Struct):
            string_field = String()

        # when, then
        assert ObjectA() == ObjectB()

    @staticmethod
    def test_two_structs_are_not_equal():
        # given
        class ObjectA(Struct):
            string_field = String()

        class ObjectC(Struct):
            string_field = String()
            extra_field = String()

        # when, then
        assert ObjectA() != ObjectC()

    @staticmethod
    def test_two_structs_with_reordered_fields_are_not_equal():
        # given
        class MyStructA(Struct):
            first = String()
            second = String()

        class MyStructB(Struct):
            second = String()
            first = String()

        # when, then
        assert MyStructA() != MyStructB()

    #
    # Arrays

    @staticmethod
    def test_two_arrays_are_equal():
        # given
        field = Array(String())
        another_field = Array(String())

        # when, then
        assert field == another_field

    @staticmethod
    def test_two_arrays_with_different_names_are_not_equal():
        # given
        field = Array(String(), name="field_name")
        another_field = Array(String(), name="different_name")

        # when, then
        assert field != another_field

    @staticmethod
    def test_two_arrays_with_different_element_types_are_not_equal():
        # given
        field = Array(String())
        another_field = Array(Float())

        # when, then
        assert field != another_field

    #
    # Test the influence of metadata on equality

    @staticmethod
    @pytest.mark.parametrize(
        "value_a, value_b, expected_is_equal",
        [
            pytest.param(String(), String(metadata={}), True, id="default versus explicit empty dict lhs"),
            pytest.param(String(metadata={}), String(), True, id="default versus explicit empty dict rhs"),
            pytest.param(String(metadata={}), String(metadata={"some_key": "some_value"}), False, id="different dicts"),
            pytest.param(String(metadata={}), String(metadata={}), True, id="both empty dicts"),
        ],
    )
    def test_comparison_by_metadata(value_a, value_b, expected_is_equal):
        # ^ given

        # when
        is_equal = value_a == value_b

        # then
        assert is_equal == expected_is_equal
