"""
Suite of tests for the `Struct.Meta.includes` feature.

Partner to `test_struct.py`.
"""

import pytest
from pyspark.sql.types import StructType, StructField, StringType

from sparkorm import String, schema, Integer, Float
from sparkorm.exceptions import InvalidStructError
from sparkorm.struct import Struct


#
# Test path and field handling


class CousinStruct(Struct):
    cousin_field = String()


class SiblingAStruct(Struct):
    class Meta:
        includes = [CousinStruct]

    sibling_a_field = String()


class SiblingBStruct(Struct):
    sibling_b_field = Float()


class RootStruct(Struct):
    class Meta:
        includes = [SiblingAStruct, SiblingBStruct]


class TestStructIncludes:
    @staticmethod
    def test_should_give_correct_path_when_referring_to_field_via_source_class():
        assert RootStruct.sibling_a_field.PATH == "sibling_a_field"
        assert RootStruct.sibling_b_field.PATH == "sibling_b_field"
        assert RootStruct.cousin_field.PATH == "cousin_field"

    @staticmethod
    def test_should_give_correct_field_object_when_referring_to_field_via_source_class():
        assert RootStruct.sibling_a_field == String(name="sibling_a_field")
        assert RootStruct.sibling_b_field == Float(name="sibling_b_field")
        assert RootStruct.cousin_field == String(name="cousin_field")


#
# Other tests


class TestStructIncludesSchemaBuilding:
    @staticmethod
    def test_should_combine_disjoint_includes():
        # given
        class AnObject(Struct):
            field_a = String()

        class AnotherObject(Struct):
            field_b = String()

        # when
        class CompositeObject(Struct):
            class Meta:
                includes = [AnObject, AnotherObject]

            native_field = String()

        composite_schema = schema(CompositeObject)

        # expect
        assert composite_schema == StructType(
            [
                StructField("native_field", StringType()),
                StructField("field_a", StringType()),
                StructField("field_b", StringType()),
            ]
        )

    @staticmethod
    def test_should_combine_overlapping_includes():
        # given
        class AnObject(Struct):
            field_z = String()

        class AnotherObject(Struct):
            field_z = String()

        # when
        class CompositeObject(Struct):
            class Meta:
                includes = [AnObject, AnotherObject]

        composite_schema = schema(CompositeObject)

        # expect
        assert composite_schema == StructType([StructField("field_z", StringType())])

    @staticmethod
    def test_should_reject_incompatible_includes_fields():
        # given
        class AnObject(Struct):
            field_z = String()

        class AnotherObject(Struct):
            field_z = Integer()

        # when, expect
        with pytest.raises(
                InvalidStructError, match="Attempting to replace a field with an 'includes' field of different type"
        ):
            class CompositeObject(Struct):
                class Meta:
                    includes = [AnObject, AnotherObject]

    @staticmethod
    def test_should_reject_non_class_in_includes():
        # given, when, expect
        with pytest.raises(
                InvalidStructError, match="Encountered non-class item in 'includes' list of 'Meta' inner class"
        ):
            class CompositeObject(Struct):
                class Meta:
                    includes = [""]

    @staticmethod
    def test_class_must_be_struct_or_struct_subclass():
        # given, when, expect
        with pytest.raises(
                InvalidStructError,
                match="Encountered item in 'includes' list of 'Meta' inner class that is not a Struct or Struct subclass",
        ):
            class CompositeObject(Struct):
                class Meta:
                    includes = [str]
