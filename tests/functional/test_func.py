import pytest

from sparkorm import Date, Decimal, Integer, String, Timestamp
from sparkorm.metadata_types import (
    LocationConfig,
    LocationType,
    MetaConfig,
    NoChangeStrategy,
)
from sparkorm.models import TableModel
from tests.functional.conftest import TestDB, class_fixture, session_fixture


class TestTable(TableModel):
    class Meta(MetaConfig):
        db_config = TestDB
        name = "test_table"

    s = String()
    i = Integer(nullable=False)


class TableFromLocation(TableModel):
    """
    This table is used to test the case when the table is in the local database
    """

    class Meta(MetaConfig):
        name = "test_table"
        db_config = TestDB
        migration_strategy = NoChangeStrategy()
        location = LocationConfig(LocationType.DELTA, "abfss://user@domain.com/path1/PATH2")

    vendor_key = String()
    invoice_date = Timestamp()
    amt = Decimal(18, 3)
    current_date = Date(nullable=False)


@pytest.mark.usefixtures(session_fixture.__name__, class_fixture.__name__)
class TestFuncTableModels:
    @pytest.fixture()
    def setup_teardown(self):
        yield

    def test_drop(self):
        test_table = TestTable(self.spark)
        test_table.create()
        df = self.spark.createDataFrame(
            [
                ("A", 1),
                ("B", 2),
                ("C", 3),
            ],
            schema=test_table.get_spark_schema(),
        )
        test_table.insert_from_df(df)

        test_table.drop()
        assert not self.spark.catalog.tableExists(test_table.get_full_name())
