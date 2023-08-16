"""
Suite of tests for Struct inheritance.

Partner to `test_struct.py`.
"""

from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

from sparkorm.exceptions import InvalidStructError
from sparkorm import String, schema, Integer, Float

import pytest

from sparkorm.struct import Struct


class TestStructInheritancePathStr:
    @staticmethod
    def test_path_str_on_child_struct():
        # given
        class ParentStruct(Struct):
            parent_field = String()

        class ChildStruct(ParentStruct):
            child_field = Float()

        # when, then
        assert ChildStruct.parent_field.PATH == "parent_field"
        assert ChildStruct.child_field.PATH == "child_field"


class TestStructInheritanceSchemaBuilding:
    @staticmethod
    def test_should_combine_disjoint_fields():
        # given
        class ParentStruct(Struct):
            parent_field = String()

        class ChildStruct(ParentStruct):
            child_field = Float()

        # when, then
        assert schema(ParentStruct) == StructType([StructField("parent_field", StringType())])
        assert schema(ChildStruct) == StructType(
            [StructField("parent_field", StringType()), StructField("child_field", FloatType())]
        )

    @staticmethod
    def test_should_inherit_includes():
        # given
        class SiblingStruct(Struct):
            sibling_field = Integer()

        class ParentStruct(Struct):
            class Meta:
                includes = [SiblingStruct]

            parent_field = String()

        class ChildStruct(ParentStruct):
            child_field = Float()

        # when
        spark_schema = schema(ChildStruct)

        # then

        assert spark_schema == StructType(
            [
                StructField("parent_field", StringType()),
                StructField("sibling_field", IntegerType()),
                StructField("child_field", FloatType()),
            ]
        )

    @staticmethod
    def test_should_allow_override_with_same_type():
        # given
        class ParentStruct(Struct):
            parent_field = String()

        class ChildStruct(ParentStruct):
            parent_field = String()

        # when, then
        assert schema(ChildStruct) == StructType([StructField("parent_field", StringType())])

    @staticmethod
    def test_should_reject_override_with_different_type():
        # given, when, then
        with pytest.raises(
            InvalidStructError, match="Attempting to replace field 'parent_field' with field of different type"
        ):

            class ParentStruct(Struct):
                parent_field = String()

            class ChildStruct(ParentStruct):
                parent_field = Float()
