import re

import pytest

from sparkorm import Array, Float, String
from sparkorm.exceptions import FieldValueValidationError
from tests.utilities import does_not_raise


class TestArrayFieldValidateOnValue:
    @staticmethod
    @pytest.mark.parametrize(
        "array_field, value, expected_error_message",
        [
            pytest.param(
                Array(Float()), {}, "Value for an array must be a sequence, not 'dict'", id="array-is-not-a-sequence"
            ),
            pytest.param(
                Array(Float()),
                [3.5, "string"],
                "Value 'string' has invalid type 'str'. Allowed types are: 'float'",
                id="element-value-of-wrong-type",
            ),
            pytest.param(
                Array(Float(nullable=False)),
                [None],
                "Encountered None value in array, but the element field of this array is specified as non-nullable",
                id="null-element-in-unnamed-array-that-accepts-nonnull-only",
            ),
        ],
    )
    def should_raise_validation_error_when(array_field, value, expected_error_message):
        # given, when, then
        with pytest.raises(FieldValueValidationError, match=re.escape(expected_error_message)):
            array_field._validate_on_value(value)

    @staticmethod
    @pytest.mark.parametrize(
        "array_field, value, expected_error",
        [
            pytest.param(
                Array(String(), name="name_of_array_field"),
                None,
                does_not_raise(),
                id="array-of-nullable-elements-should-accept-none",
            ),
            pytest.param(
                Array(String(), name="name_of_array_field"),
                "this is a string value",
                pytest.raises(
                    FieldValueValidationError,
                    match=re.escape(
                        "Value for an array must not be a string. Found value 'this is a string value'. Did you mean "
                        "to use a list of strings?"
                    ),
                ),
                id="string-array-should-reject-non-sequence",
            ),
            pytest.param(
                Array(Float(), name="name_of_array_field"),
                3.5,
                pytest.raises(
                    FieldValueValidationError, match=re.escape("Value for an array must be a sequence, not 'float'")
                ),
                id="float-array-should-reject-non-sequence",
            ),
            pytest.param(
                Array(Float(nullable=False), name="name_of_array_field"),
                [None],
                pytest.raises(
                    FieldValueValidationError,
                    match=re.escape(
                        "Encountered None value in array, but the element field of this array is specified as "
                        "non-nullable (array field name = 'name_of_array_field')"
                    ),
                ),
                id="float-array-of-non-nullable-elements-should-reject-null-element",
            ),
        ],
    )
    def test_arrays_should_be_handled_correctly(array_field: Array, value, expected_error):
        # given, when, then
        with expected_error:
            array_field._validate_on_value(value)
