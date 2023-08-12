import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as sql_funcs
from pyspark.sql.types import StructField, StringType

from sparkorm import String, Struct, Array
from sparkorm import accessors


class User(Struct):
    full_name = String(nullable=False)
    bio = String(name="biography", nullable=False, metadata={"description": "Short biography"})


class Article(Struct):
    author = User(metadata={})


class Message(Struct):
    sender = User()
    recipients = Array(User())


class TestPathSeq:
    @staticmethod
    def test_should_return_correct_list_for_nested_schema():
        # given (see above)

        # when
        path_field_names = accessors.path_seq(Article.author.full_name)

        # then
        assert path_field_names == ["author", "full_name"]

    @staticmethod
    def test_should_return_correct_list_for_array():
        # given (see above)

        # when
        path_field_names = accessors.path_seq(Message.recipients.e.full_name)

        # then
        assert path_field_names == ["recipients", "full_name"]

    @staticmethod
    def test_should_return_correct_list_for_nested_array_with_explicit_field_name():
        # given (see above)
        class Element(Struct):
            element_field = String(name="alt_element_field_name", nullable=True)

        class StructWithArray(Struct):
            array_field = Array(Element(), name="alt_array_field_name", nullable=True)

        # when
        path_field_names = accessors.path_seq(StructWithArray.array_field.e.element_field)

        # then
        assert path_field_names == ["alt_array_field_name", "alt_element_field_name"]


class TestPathStr:
    @staticmethod
    def test_should_return_correct_str_for_nested_schema():
        # given (see above)

        # when
        path_field_names = accessors.path_str(Article.author.full_name)

        # then
        assert path_field_names == "author.full_name"

    @staticmethod
    def test_should_return_correct_str_for_array():
        # given (see above)

        # when
        path_field_names = accessors.path_str(Message.recipients.e.full_name)

        # then
        assert path_field_names == "recipients.full_name"


class TestPathCol:
    @staticmethod
    def test_should_return_correct_column_for_nested_schemas(spark_session: SparkSession):
        # spark_session: Testing of `path_col` has implicit JVM Spark dependency

        # given (see above)

        # when
        col_ref = accessors.path_col(Article.author.full_name)

        # then
        assert str(col_ref) == str(sql_funcs.col("author")["full_name"])


class TestPathCol:
    @staticmethod
    def test_should_return_correct_column_for_nested_schemas(spark_session: SparkSession):
        # spark_session: Testing of `path_col` has implicit JVM Spark dependency

        # given (see above)

        # when
        col_ref = accessors.path_col(Article.author.full_name)

        # then
        assert str(col_ref) == str(sql_funcs.col("author")["full_name"])


class TestName:
    @staticmethod
    def test_field_name_is_correct_from_explicit_name():
        # given (see above)

        # when
        field_name = accessors.name(Article.author.bio)

        # then
        assert field_name == "biography"

    @staticmethod
    def test_field_name_is_correct_from_implicit_name():
        # given (see above)

        # when
        field_name = accessors.name(Article.author.full_name)

        # then
        assert field_name == "full_name"


class TestMetadata:
    @staticmethod
    @pytest.mark.parametrize(
        "input_field, expected_metadata",
        [
            pytest.param(Article.author, {}, id="empty dictionary metadata"),
            pytest.param(User.bio, {"description": "Short biography"}, id="metadata with single field"),
            pytest.param(User.full_name, {}, id="unset metadata"),
        ],
    )
    def test_metadata_is_correct(input_field, expected_metadata):
        # given (see above)

        # when
        metadata = accessors.metadata(input_field)

        # then
        assert metadata == expected_metadata


class TestStructField:
    @staticmethod
    def test_struct_field_is_correct():
        # given (see above)

        # when
        struct_field = accessors.struct_field(Article.author.bio)

        # then
        assert struct_field == StructField("biography", StringType(), False, {"description": "Short biography"})
