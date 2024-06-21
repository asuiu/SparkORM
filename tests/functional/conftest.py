import pytest

from sparkorm.metadata_types import DBConfig
from tests.functional.connector import SparkConnector

DEFAULT_DB_NAME = "test_db"


class TestDB(DBConfig):
    @classmethod
    def get_name(cls) -> str:
        return DEFAULT_DB_NAME


@pytest.fixture(scope="session", autouse=True)
def session_fixture(request):  # pylint: disable=unused-argument
    connector = SparkConnector()
    db = TestDB(connector.spark)
    db.ensure_exists()

    yield connector

    # teardown logic here
    db.drop()
    connector.spark.stop()


@pytest.fixture(scope="class")
def class_fixture(request, session_fixture: SparkConnector):  # pylint: disable=redefined-outer-name,unused-argument
    connector = SparkConnector()
    request.cls.spark = connector.spark
    yield
