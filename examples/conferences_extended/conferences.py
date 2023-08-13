from datetime import datetime

from sparkorm import Float, String, Timestamp, Array, Struct
from sparkorm.models import TableModel


class Location(Struct):
    lat = Float(False, name="latitude")
    lon = Float(False, name="longitude")


class Conference(TableModel):
    name = String(False)
    city = String(False)
    city_location = Location(False)
    country = String(False)
    twitter = String()
    start_date = Timestamp(False)
    end_date = Timestamp(False)
    url = String()
    keywords = Array(String(False), True)


prettified_schema = """
StructType([
    StructField('name', StringType(), False), 
    StructField('city', StringType(), False), 
    StructField('city_location', 
        StructType([
            StructField('latitude', FloatType(), False), 
            StructField('longitude', FloatType(), False)]), 
        False), 
    StructField('country', StringType(), False), 
    StructField('twitter', StringType(), True), 
    StructField('start_date', TimestampType(), False), 
    StructField('end_date', TimestampType(), False), 
    StructField('url', StringType(), True), 
    StructField('keywords', 
        ArrayType(StringType(), False), 
        True)])
"""


cardiff_pycon_dict = Conference.make_dict(
    name="PyCon UK",
    city="Cardiff",
    city_location=Location.make_dict(
        lat=51.48,
        lon=-3.18
    ),
    country="GBR",
    twitter=None,
    start_date=datetime(2019, 9, 13, 0, 0),
    end_date=datetime(2019, 9, 17, 13, 23, 59),
    url=None,
    keywords=["python", "software"]
)
