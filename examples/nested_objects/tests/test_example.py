from pyspark.sql import Column
from pyspark.sql.functions import col

from examples.nested_objects.sparkorm_example import author_city_str, author_city_col, comment_usernames_str, \
    comment_usernames_col, comment_messages_str, comment_messages_col


def test_all():
    assert author_city_str == "author.address.city"
    assert_cols_equal(author_city_col, col("author")["address"]["city"])

    assert comment_usernames_str == "comments.author.username"
    assert_cols_equal(comment_usernames_col, col("comments")["author"]["username"])

    assert comment_messages_str == "comments.message"
    assert_cols_equal(comment_messages_col, col("comments")["message"])


def assert_cols_equal(col_a: Column, col_b):
    assert str(col_a) == str(col_b)
