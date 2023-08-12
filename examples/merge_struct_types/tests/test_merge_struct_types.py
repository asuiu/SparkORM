from sparkorm import pretty_schema
from ..merge_struct_types import merged_schema, pretty_merged_schema


def test_merge_struct_types():
    assert pretty_schema(merged_schema) == pretty_merged_schema.strip()
