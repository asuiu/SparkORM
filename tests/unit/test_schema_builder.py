from pyspark.sql.types import StructField, StructType, StringType, FloatType, TimestampType, ArrayType

from sparkorm.struct import Struct
from sparkorm import String, Array, Float, Timestamp, schema


class TestSparkSchema:
    @staticmethod
    def test_should_structise_flat_object():
        # given
        class User(Struct):
            id = String(nullable=False)
            age = Float()
            full_name = String(name="name")

        # when
        struct = schema(User)

        # then
        assert struct == StructType(
            [
                StructField("id", StringType(), nullable=False),
                StructField("age", FloatType()),
                StructField("name", StringType()),
            ]
        )

    @staticmethod
    def test_should_structise_deep_object():
        # given
        class User(Struct):
            id = String(nullable=False)
            age = Float()
            full_name = String(name="name")

        class Article(Struct):
            author = User(name="article_author", nullable=False)
            title = String(nullable=False)
            date = Timestamp()

        # when
        struct = schema(Article)

        # then
        assert struct == StructType(
            [
                StructField(
                    "article_author",
                    nullable=False,
                    dataType=StructType(
                        [
                            StructField("id", StringType(), nullable=False),
                            StructField("age", FloatType()),
                            StructField("name", StringType()),
                        ]
                    ),
                ),
                StructField("title", StringType(), nullable=False),
                StructField("date", TimestampType()),
            ]
        )

    @staticmethod
    def test_should_structise_object_containing_array_of_objects():
        # given
        class Tag(Struct):
            id = String(nullable=False)
            name = String()

        class Article(Struct):
            id = String(nullable=False)
            tags = Array(Tag(nullable=True))

        # when
        struct = schema(Article)

        # then
        assert struct == StructType(
            [
                StructField("id", StringType(), nullable=False),
                StructField(
                    "tags",
                    ArrayType(
                        containsNull=True,
                        elementType=StructType(
                            [StructField("id", StringType(), nullable=False), StructField("name", StringType())]
                        ),
                    ),
                    nullable=True,
                ),
            ]
        )
