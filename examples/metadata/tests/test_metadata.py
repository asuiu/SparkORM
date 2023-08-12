from sparkorm import pretty_schema, schema
from .. import metadata


def test_sparkorm_stringified_schema():
    # given

    # when
    generated_schema = pretty_schema(schema(metadata.Article))

    # then
    assert generated_schema == metadata.prettified_schema.strip()
