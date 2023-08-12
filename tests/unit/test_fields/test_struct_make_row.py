"""
Suite of tests for Struct make_dict.

Partner to `test_struct.py`.
"""

import re
from collections import OrderedDict
from typing import Mapping, Any

import pytest

from sparkorm.exceptions import StructInstantiationArgumentsError
from sparkorm import Struct, String, Float


def assert_ordered_dicts_equal(dict_a: Mapping[Any, Any], dict_b: Mapping[Any, Any]):
    assert dict_a == dict_b
    assert OrderedDict(dict_a) == OrderedDict(dict_b)


class TestStructMakeRow:
    @staticmethod
    def should_not_be_implemented():
        class AnObject(Struct):
            pass

        with pytest.raises(Exception):
            AnObject.make_row()
