from sparkorm import pretty_schema, schema
from examples.conferences_comparison import plain_schema, sparkorm_schema


def test_plain_stringified_schema():
    # given

    # when
    generated_schema = pretty_schema(plain_schema.CONFERENCE_SCHEMA)

    # then
    assert generated_schema == plain_schema.prettified_schema.strip()


def test_plain_munged_data():
    # given

    # when
    actual_rows = [row.asDict(True) for row in plain_schema.dframe.collect()]

    # then
    assert actual_rows == plain_schema.expected_rows


def test_sparkorm_stringified_schema():
    # given

    # when
    generated_schema = pretty_schema(schema(sparkorm_schema.Conference))

    # then
    assert generated_schema == sparkorm_schema.prettified_schema.strip()


def test_sparkorm_munged_data():
    # given

    # when
    actual_rows = [row.asDict(True) for row in sparkorm_schema.dframe.collect()]

    # then
    assert actual_rows == sparkorm_schema.expected_rows


def test_schemas_are_equivalent():
    assert schema(sparkorm_schema.Conference) == plain_schema.CONFERENCE_SCHEMA
