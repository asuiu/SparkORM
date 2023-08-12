import pytest
from pyspark.sql import SparkSession

from sparkorm.exceptions import InvalidDataFrameError
from sparkorm import Struct, String, Decimal


def test_validation_example(spark_session: SparkSession):
    dframe = spark_session.createDataFrame([{"title": "abc", "print_price_usd": 1.99}])

    class Article(Struct):
        title = String()
        body = String()
        print_price_usd = Decimal(100, 3)

    validation_result = Article.validate_data_frame(dframe)

    assert not validation_result.is_valid
    assert validation_result.report == """Struct schema...

StructType([
    StructField('title', StringType(), True), 
    StructField('body', StringType(), True), 
    StructField('print_price_usd', DecimalType(), True)])

DataFrame schema...

StructType([
    StructField('print_price_usd', DoubleType(), True), 
    StructField('title', StringType(), True)])

Diff of struct -> data frame...

  StructType([
-     StructField('print_price_usd', DoubleType(), True), 
-     StructField('title', StringType(), True)])
+     StructField('title', StringType(), True), 
+     StructField('body', StringType(), True), 
+     StructField('print_price_usd', DecimalType(), True)])"""

    with pytest.raises(InvalidDataFrameError):
        Article.validate_data_frame(dframe).raise_on_invalid()
