from sparkorm import pretty_schema, schema
from .. import arrays


def test_sparkorm_stringified_schema():
    # given

    # when
    generated_schema = pretty_schema(schema(arrays.Article))

    # then
    assert generated_schema == arrays.prettified_schema.strip()
