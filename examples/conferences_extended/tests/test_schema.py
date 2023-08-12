from datetime import datetime
from collections import OrderedDict

from sparkorm import schema, pretty_schema
from examples.conferences_extended.conferences import Conference, prettified_schema, cardiff_pycon_dict


def test_stringified_schema():
    # given

    # when
    generated_schema = pretty_schema(schema(Conference))

    # then
    assert generated_schema == prettified_schema.strip()


def test_dict():
    print(dict(cardiff_pycon_dict))
    assert cardiff_pycon_dict == OrderedDict(
        {
            'name': 'PyCon UK',
            'city': 'Cardiff',
            'city_location': OrderedDict({
                'latitude': 51.48,
                'longitude': -3.18
            }),
            'country': 'GBR',
            'twitter': None,
            'start_date': datetime(2019, 9, 13, 0, 0),
            'end_date': datetime(2019, 9, 17, 13, 23, 59),
            'url': None,
            'keywords': ['python', 'software']}
    )
