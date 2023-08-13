from sparkorm import Array, String
from sparkorm.models import TableModel


#
# Example of attaching metadata to a field

class Article(TableModel):
    title = String(nullable=False, metadata={'description': 'Title of the article'})
    tags = Array(String(), nullable=False, metadata={'enum': ['spark', 'sparkorm']})
    comments = Array(String(nullable=False), metadata={'max_length': 100})


#
# Here's what the schema looks like

prettified_schema = """
StructType([
    StructField('title', StringType(), False, {'description': 'Title of the article'}), 
    StructField('tags', 
        ArrayType(StringType(), True), 
        False, 
        {'enum': ['spark', 'sparkorm']}), 
    StructField('comments', 
        ArrayType(StringType(), False), 
        True, 
        {'max_length': 100})])
"""
