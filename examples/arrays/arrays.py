from sparkorm import Struct, Array, String


#
# Simple example of the nullability of an array field vs
# nullability of elements in an array

class Article(Struct):
    title = String(nullable=False)
    tags = Array(String(), nullable=False)
    comments = Array(String(nullable=False))


#
# Here's what the schema looks like

prettified_schema = """
StructType([
    StructField('title', StringType(), False), 
    StructField('tags', 
        ArrayType(StringType(), True), 
        False), 
    StructField('comments', 
        ArrayType(StringType(), False), 
        True)])
"""
