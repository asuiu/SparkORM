"""
Suite of tests for Struct "implements".

Partner to `test_struct.py`.
"""

from sparkorm import Struct, String
from sparkorm.exceptions import StructImplementationError

import pytest


class SimpleStruct(Struct):
    field_a = String()


class TestStructImplements:
    @staticmethod
    def should_reject_due_to_missing_fields():
        # given, when, then
        with pytest.raises(StructImplementationError) as err_info:

            class ExampleStruct(Struct):
                class Meta:
                    implements = [SimpleStruct]

        assert str(err_info.value) == (
            "Struct 'ExampleStruct' does not implement field 'field_a' required by struct 'SimpleStruct'"
        )

    @staticmethod
    def should_reject_due_to_incorrect_field_type():
        # given, when, then
        with pytest.raises(StructImplementationError) as err_info:

            class ExampleStruct(Struct):
                class Meta:
                    implements = [SimpleStruct]

                field_a = String(nullable=False)

        assert str(err_info.value) == (
            "Struct 'ExampleStruct' implements field 'field_a' (required by struct 'SimpleStruct') but "
            "field is not compatible. Required <Nullable String: field_a> but found <String: field_a>"
        )

    @staticmethod
    def should_allow_struct_that_correctly_implements_fields():
        # given, when, then
        class ExampleStruct(Struct):
            class Meta:
                implements = [SimpleStruct]

            field_a = String()

    @staticmethod
    def should_allow_struct_whose_superclass_implements_fields():
        # given, when, then
        class SimpleBase(Struct):
            field_a = String()

        class ExampleStruct(SimpleBase):
            class Meta:
                implements = [SimpleStruct]
