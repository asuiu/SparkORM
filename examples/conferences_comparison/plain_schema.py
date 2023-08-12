from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, Row

#
# A simple schema for conferences and their locations

CITY_SCHEMA = StructType()
CITY_NAME_FIELD = "name"
CITY_SCHEMA.add(StructField(CITY_NAME_FIELD, StringType(), False))
CITY_LAT_FIELD = "latitude"
CITY_SCHEMA.add(StructField(CITY_LAT_FIELD, FloatType()))
CITY_LONG_FIELD = "longitude"
CITY_SCHEMA.add(StructField(CITY_LONG_FIELD, FloatType()))

CONFERENCE_SCHEMA = StructType()
CONF_NAME_FIELD = "name"
CONFERENCE_SCHEMA.add(StructField(CONF_NAME_FIELD, StringType(), False))
CONF_CITY_FIELD = "city"
CONFERENCE_SCHEMA.add(StructField(CONF_CITY_FIELD, CITY_SCHEMA))

#
# Here's what the schema looks like

prettified_schema = """
StructType([
    StructField('name', StringType(), False), 
    StructField('city', 
        StructType([
            StructField('name', StringType(), False), 
            StructField('latitude', FloatType(), True), 
            StructField('longitude', FloatType(), True)]), 
        True)])
"""

#
# Create a data frame with some dummy data

spark = SparkSession.builder.appName("conferences-comparison-demo").getOrCreate()
dframe = spark.createDataFrame([
    {
        CONF_NAME_FIELD: "PyCon UK 2019",
        CONF_CITY_FIELD: {CITY_NAME_FIELD: "Cardiff", CITY_LAT_FIELD: 51.48, CITY_LONG_FIELD: -3.18}
    }],
    schema=CONFERENCE_SCHEMA)

#
# Munge some data

dframe = dframe.withColumn("city_name", dframe[CONF_CITY_FIELD][CITY_NAME_FIELD])

#
# Here's what the output looks like

expected_rows = [
    {'name': 'PyCon UK 2019', 'city': {'name': 'Cardiff', 'latitude': 51.47999954223633, 'longitude': -3.180000066757202}, 'city_name': 'Cardiff'}
]
