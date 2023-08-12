from pyspark.sql import SparkSession

from sparkorm import Struct, String, Float, schema, path_col


#
# A simple schema for conferences and their locations

class City(Struct):
    name = String(nullable=False)
    latitude = Float()
    longitude = Float()


class Conference(Struct):
    name = String(nullable=False)
    city = City()

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
        str(Conference.name): "PyCon UK 2019",
        str(Conference.city): {
            str(City.name): "Cardiff",
            str(City.latitude): 51.48,
            str(City.longitude): -3.18
        }
    }],
    schema=schema(Conference))

#
# Munge some data

dframe = dframe.withColumn("city_name", path_col(Conference.city.name))

#
# Here's what the output looks like

expected_rows = [
    {'name': 'PyCon UK 2019', 'city': {'name': 'Cardiff', 'latitude': 51.47999954223633, 'longitude': -3.180000066757202}, 'city_name': 'Cardiff'}
]
