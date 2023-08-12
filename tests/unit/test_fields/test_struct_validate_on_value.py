import re

import pytest

from sparkorm.exceptions import FieldValueValidationError
from sparkorm import Struct, String


class TestStructValidateOnValue:
    @staticmethod
    @pytest.mark.parametrize(
        "value, expected_error",
        [
            pytest.param(
                [],
                pytest.raises(
                    FieldValueValidationError, match=re.escape("Value for a struct must be a mapping, not 'list'")
                ),
                id="value-is-wrong-type",
            ),
            pytest.param(
                {},
                pytest.raises(
                    FieldValueValidationError,
                    match=re.escape(
                        "Dict has incorrect number of fields. \nStruct requires 1 fields: text \nDict has 0 fields: "
                    ),
                ),
                id="not-enough-fields",
            ),
            pytest.param(
                {"wrong_name": "hello"},
                pytest.raises(
                    FieldValueValidationError,
                    match=re.escape(
                        "Dict fields do not match struct fields. \nStruct fields: text \nDict fields: wrong_name"
                    ),
                ),
                id="incorrect-field-name",
            ),
            pytest.param(
                {"text": 3},
                pytest.raises(
                    FieldValueValidationError,
                    match=re.escape("Value '3' has invalid type 'int'. Allowed types are: 'str'"),
                ),
                id="incorrect-field-type",
            ),
        ],
    )
    def should_reject_invalid_values(value, expected_error):
        # given
        class SimpleObject(Struct):
            text = String()

        struct = SimpleObject()

        # when
        with expected_error:
            struct._validate_on_value(value)

    @staticmethod
    def should_allow_null_struct_in_nullable_field():
        # given
        class NestedObject(Struct):
            nested_text = String

        class SimpleObject(Struct):
            object_field = NestedObject()

        struct = SimpleObject()

        value = {"object_field": None}

        # when, then
        struct._validate_on_value(value)

    @staticmethod
    def should_reject_null_struct_in_non_nullable_field():
        # given
        class NestedObject(Struct):
            nested_text = String

        class SimpleObject(Struct):
            object_field = NestedObject(nullable=False)

        struct = SimpleObject()

        value = {"object_field": None}

        # when, then
        with pytest.raises(
            FieldValueValidationError,
            match=re.escape("Non-nullable field cannot have None value (field name = 'object_field')"),
        ):
            struct._validate_on_value(value)
