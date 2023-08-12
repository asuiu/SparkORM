import pytest

from sparkorm import pretty_schema, schema
from sparkorm.exceptions import StructImplementationError

from examples.composite_schemas import inheritance, includes


def test_inheritance_registration_schema():
    generated_schema = pretty_schema(schema(inheritance.RegistrationEvent))
    assert generated_schema == inheritance.prettified_registration_event_schema.strip()


def test_includes_registration_schema():
    generated_schema = pretty_schema(schema(includes.RegistrationEvent))
    assert generated_schema == includes.prettified_registration_event_schema.strip()


def test_implements_registration_schema_should_fail():
    with pytest.raises(StructImplementationError) as err_info:
        from .. import implements
    assert str(err_info.value) == (
        "Struct 'PageViewLogEntry' does not implement field 'logged_at' required by struct 'LogEntryMetadata'")
