from io import StringIO
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pyspark.pandas import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.catalog import Column
from pyspark.sql.types import DecimalType, Row, StringType, StructField, TimestampType, DateType

from sparkorm import Decimal, String, Date, Timestamp, Map, Array
from sparkorm.exceptions import TableUpdateError
from sparkorm.metadata_types import DBConfig, NoChangeStrategy, DropAndCreateStrategy, MetaConfig, SchemaUpdateStatus
from sparkorm.models import TableModel, ViewModel, BaseModel
from tests.utilities import convert_to_spark_types

DEFAULT_DB_NAME = "db_test_test"


class TestDB(DBConfig):
    @classmethod
    def get_name(cls) -> str:
        return "test_db"


class TestTable(TableModel):
    class Meta:
        db_config = TestDB
        name = "test_table"

    vendor_key = String()
    invoice_date = Timestamp()
    amt = Decimal(18, 3)
    current_date = Date(nullable=False)


class LocalTable(TableModel):
    """
    This table is used to test the case when the table is in the local database
    """

    class Meta:
        name = "test_table"
        migration_strategy = NoChangeStrategy()

    vendor_key = String()
    invoice_date = Timestamp()
    amt = Decimal(18, 3)
    current_date = Date(nullable=False)


class DropCreateStrategyTable(TableModel):
    """
    This table is used to test the case when the table is in the local database
    """

    class Meta(MetaConfig):
        name = "test_table"
        migration_strategy = DropAndCreateStrategy()

    vendor_key = String(partitioned_by=True)
    amt = Decimal(18, 3)


class LocalTableNoVendorKey(TableModel):
    class Meta:
        name = "test_table"

    # vendor_key = String()
    invoice_date = Timestamp()
    amt = Decimal(18, 3)
    current_date = Date()


class TestPartitionedTable(TableModel):
    class Meta:
        name = "test_p_table"

    vendor_key = String()
    invoice_date = Timestamp(partitioned_by=True)
    amt = Decimal(18, 3)
    current_date = Date(nullable=False, partitioned_by=True)


class TestView(ViewModel):
    class Meta:
        db_config = TestDB
        name = "test_view"


class TestTableModels:
    @pytest.fixture(scope="function")
    def setup_clean_spark_catalog(self, spark_session: SparkSession):
        # Assure no tables/views exist before the test
        assert not spark_session.catalog.listTables()

        yield  # This allows the test to run

        # Clean up tables/views after the test
        for table in spark_session.catalog.listTables():
            try:
                spark_session.catalog.dropTempView(table.name)
            except:
                pass
        assert not spark_session.catalog.listTables()

    def test_raise_exception_on_invalid_meta(self):
        with pytest.raises(AssertionError):

            class InvalidMeta(BaseModel):
                class Meta:
                    unknown_var = "test_table"

    def test_partitioned_by_properly_set(self):
        schema = TestPartitionedTable.get_spark_schema()
        partitioned_by_fields = [
            field.name for field in schema.fields if field.metadata.get("partitioned_by", False) is True
        ]
        assert partitioned_by_fields == ["invoice_date", "current_date"]

    def test_get_spark_schema_nominal(self):
        schema = TestPartitionedTable.get_spark_schema()
        actual_fields = schema.fields
        expected_fields = [
            StructField("vendor_key", StringType(), True),
            StructField("invoice_date", TimestampType(), True, metadata={"partitioned_by": True}),
            StructField("amt", DecimalType(18, 3), True),
            StructField("current_date", DateType(), False, metadata={"partitioned_by": True}),
        ]

        # check that every element in expected and in actual are equal
        assert all([x == y for x, y in zip(actual_fields, expected_fields)])

    def test_get_spark_schema_adds_partitioned_by_metadata(self):
        schema = TestPartitionedTable.get_spark_schema()
        schema.fields[0].metadata = {"partitioned_by": False}

    def test_get_full_name(self):
        actual = TestTable.get_full_name()
        assert "test_db.test_table" == actual

    def test_ensure_exists_table_happy_path(self):
        spark_mock = MagicMock(spec=SparkSession)
        spark_mock.catalog.tableExists.return_value = False
        exists = TestTable(spark_mock).ensure_exists()
        assert exists is SchemaUpdateStatus.CREATED
        spark_mock.sql.assert_called_once_with(
            "CREATE TABLE test_db.test_table (vendor_key STRING,invoice_date TIMESTAMP,amt DECIMAL(18,3),current_date DATE NOT NULL)"
        )

    def test_ensure_exists_local_table(self):
        spark_mock = MagicMock(spec=SparkSession)
        spark_mock.catalog.tableExists.return_value = False
        exists = LocalTable(spark_mock).ensure_exists()
        assert exists is SchemaUpdateStatus.CREATED
        spark_mock.sql.assert_called_once_with(
            "CREATE TABLE test_table (vendor_key STRING,invoice_date TIMESTAMP,amt DECIMAL(18,3),current_date DATE NOT NULL)"
        )

    def test_ensure_exists_table_create_with_partitions(self):
        spark_mock = MagicMock(spec=SparkSession)
        spark_mock.catalog.tableExists.return_value = False
        exists = TestPartitionedTable(spark_mock).ensure_exists()
        assert exists is SchemaUpdateStatus.CREATED
        spark_mock.sql.assert_called_once_with(
            "CREATE TABLE test_p_table (vendor_key STRING,invoice_date TIMESTAMP,amt DECIMAL(18,3),current_date DATE NOT NULL)"
            " PARTITIONED BY (invoice_date,current_date)"
        )

    def test_ensure_exists_created_with_partitioned_by(self):
        spark_mock = MagicMock(spec=SparkSession)
        spark_mock.catalog.tableExists.return_value = False
        exists = TestPartitionedTable(spark_mock).ensure_exists()
        assert exists is SchemaUpdateStatus.CREATED
        spark_mock.sql.assert_called_once_with(
            "CREATE TABLE test_p_table (vendor_key STRING,invoice_date TIMESTAMP,amt DECIMAL(18,3),current_date DATE NOT NULL) PARTITIONED BY (invoice_date,current_date)"
        )

    def test_ensure_exists_table_does_nothing_if_exists(self, setup_clean_spark_catalog, spark_session: SparkSession):
        TestTableRow = Row(*LocalTable.get_spark_schema().names)
        data = [
            TestTableRow("VendorA", "2023-01-01 12:00:00", 123.456, "2023-01-01"),
            TestTableRow("VendorB", "2023-02-01 14:00:00", 789.101, "2023-02-01"),
        ]
        rows = convert_to_spark_types(data, LocalTable.get_spark_schema())
        df = spark_session.createDataFrame(rows, schema=LocalTable.get_spark_schema())
        df.createOrReplaceTempView(LocalTable.get_full_name())
        exists = LocalTable(spark_session).ensure_exists()
        assert exists is SchemaUpdateStatus.SKIPPED

    def test_ensure_exists_table_raises_on_distinct_table_exists(
        self, setup_clean_spark_catalog, spark_session: SparkSession
    ):
        """
        We expect the create method to raise an error if the table exists with a different schema.
        In our case we'll pass the same schema, but because we can't create real tables in the test environment, there will be view, which is not partitioned,
            and the lack of partitioned fields will cause the error.
        """
        TestTableRow = Row(*TestPartitionedTable.get_spark_schema().names)
        data = [
            TestTableRow("VendorA", "2023-01-01 12:00:00", 123.456, "2023-01-01"),
            TestTableRow("VendorB", "2023-02-01 14:00:00", 789.101, "2023-02-01"),
        ]
        rows = convert_to_spark_types(data, TestPartitionedTable.get_spark_schema())
        df = spark_session.createDataFrame(rows, schema=TestPartitionedTable.get_spark_schema())
        df.createOrReplaceTempView(TestPartitionedTable.get_full_name())
        with pytest.raises(TableUpdateError):
            TestPartitionedTable(spark_session).ensure_exists()

    def test_truncate(self):
        spark_mock = MagicMock(spec=SparkSession)
        TestTable(spark_mock).truncate()
        spark_mock.sql.assert_called_once_with("TRUNCATE TABLE test_db.test_table")

    def test_drop(self):
        spark_mock = MagicMock(spec=SparkSession)
        TestTable(spark_mock).drop()
        spark_mock.sql.assert_called_once_with("DROP TABLE test_db.test_table")

    def test_insert_batch_size_2(self):
        spark_mock = MagicMock(spec=SparkSession)
        values = [
            ['"VendorA"', '"2023-01-01 12:00:00"', "123.456", '"2023-01-01"'],
            ['"VendorB"', '"2023-02-01 14:00:00"', "789.101", '"2023-02-01"'],
            ['"VendorC"', '"2023-03-01 16:00:00"', "101.112", '"2023-03-01"'],
        ]
        TestTable(spark_mock).insert(values, batch_size=2)
        actual_insert_stms = [call[0][0] for call in spark_mock.sql.call_args_list]
        expected_stms = [
            'INSERT INTO test_db.test_table ( vendor_key,invoice_date,amt,current_date ) VALUES ("VendorA","2023-01-01 12:00:00",123.456,"2023-01-01"),("VendorB","2023-02-01 14:00:00",789.101,"2023-02-01")',
            'INSERT INTO test_db.test_table ( vendor_key,invoice_date,amt,current_date ) VALUES ("VendorC","2023-03-01 16:00:00",101.112,"2023-03-01")',
        ]

        assert actual_insert_stms == expected_stms

    def test_insert_from_csv(self):
        spark_mock = MagicMock(spec=SparkSession)

        # CSV will contain a distinct order vs the table schema
        columns = ["vendor_key", "amt", "invoice_date", "current_date"]
        data = ['"VendorA"', 123.456, '"2023-01-01 12:00:00"', "DATE(2023,01,01)"]
        df = pd.DataFrame([data], columns=columns)

        csv_file = StringIO()
        df.to_csv(csv_file, index=False)
        csv_content = csv_file.getvalue()
        csv_file = StringIO(csv_content)
        TestTable(spark_mock).insert_from_csv(csv_file)
        actual_insert_stms = [call[0][0] for call in spark_mock.sql.call_args_list]
        expected_stms = [
            'INSERT INTO test_db.test_table ( vendor_key,invoice_date,amt,current_date ) VALUES ("VendorA","2023-01-01 12:00:00",123.456,DATE(2023,01,01))'
        ]

        assert actual_insert_stms == expected_stms

    def test_insert_from_select(self):
        spark_mock = MagicMock(spec=SparkSession)
        mock_return_df = MagicMock(spec=DataFrame)
        spark_mock.sql.return_value = mock_return_df
        select_statement = "SELECT * FROM test_db2.test_table2"
        expected_df = TestTable(spark_mock).insert_from_select(select_statement)
        spark_mock.sql.assert_called_once_with(f"INSERT INTO test_db.test_table {select_statement}")
        assert expected_df is mock_return_df

    # ignore this test
    @pytest.mark.skip(
        reason="Skip this test due to the impossibility of local Spark instance to create tables without an installed Hadoop"
    )
    def test_insert_from_df(self, setup_clean_spark_catalog, spark_session: SparkSession):
        """Tests if the insert_from_df() works properly when receiving a DataFrame with default "error" mode"""
        # DropCreateStrategyTable(spark_session).ensure_exists()
        schema = DropCreateStrategyTable.get_spark_schema()
        full_name = DropCreateStrategyTable.get_full_name()
        TestTableRow = Row(*schema.names)
        data = [
            TestTableRow("VendorA", 123.456),
            TestTableRow("VendorB", 789.101),
        ]
        rows = convert_to_spark_types(data, schema)
        df = spark_session.createDataFrame(rows, schema=schema)
        df.createOrReplaceTempView(full_name)  # Create a temporary view
        DropCreateStrategyTable(spark_session).insert_from_df(df)
        assert spark_session.catalog.listTables() == [DropCreateStrategyTable.get_full_name()]

    def test_drop_create_strategy(self):
        """
        We expect the create method to raise an error if the table exists with a different schema.
        In our case we'll pass the same schema, but because we can't create real tables in the test environment, there will be view, which is not partitioned,
            and the lack of partitioned fields will cause the error.
        """
        spark_mock = MagicMock(spec=SparkSession)
        spark_mock.catalog.tableExists.return_value = True
        spark_mock.catalog.listColumns.return_value = [
            Column(
                name="vendor_key", description=None, dataType="string", nullable=True, isPartition=False, isBucket=False
            ),
            Column(
                name="amt", description=None, dataType="decimal(18,3)", nullable=True, isPartition=False, isBucket=False
            ),
        ]

        table_model_in_test = DropCreateStrategyTable(spark_mock)
        table_model_in_test.create = MagicMock()
        table_model_in_test.drop = MagicMock()

        result = table_model_in_test.ensure_exists()
        table_model_in_test.drop.assert_called_once()
        table_model_in_test.create.assert_called_once()
        assert result is SchemaUpdateStatus.DROPPED_AND_CREATED

    def test_long_names(self):
        class ScenarioFilter(TableModel):
            class Meta:
                name = "scenario_filter"
                db_config = TestDB
                migration_strategy = DropAndCreateStrategy()

            scenario_no = String()
            domain = String()
            scenario_filter_table_name = String()
            scenario_filter_column_name = Map(key=String(), value=String())
            scenario_filter_value = Map(key=String(), value=Array(element=Array(element=String())))
            filter_include_exclude_flag = String()
            scenario_filter_active_flag = String()
            comments = String()
            change_begin_date = String()
            change_end_date = String()
            last_update_date = String()

        spark_mock = MagicMock(spec=SparkSession)
        table = ScenarioFilter(spark_mock)
        columns_order = [x.name for x in table.get_spark_schema().fields]
        expected_columns_order = [
            "scenario_no",
            "domain",
            "scenario_filter_table_name",
            "scenario_filter_column_name",
            "scenario_filter_value",
            "filter_include_exclude_flag",
            "scenario_filter_active_flag",
            "comments",
            "change_begin_date",
            "change_end_date",
            "last_update_date",
        ]
        assert columns_order == expected_columns_order

    def test_sql_nominal(self):
        spark_mock = MagicMock(spec=SparkSession)
        spark_mock.sql.return_value = "test"
        TestTable(spark_mock).sql(f"SELECT * FROM {TestTable.get_full_name()}")
        spark_mock.sql.assert_called_once_with("SELECT * FROM test_db.test_table", None)


class TestViewModels:
    def test_ensure_exists_view_happy_path(self):
        spark_mock = MagicMock(spec=SparkSession)
        select_statement = "SELECT * FROM test_db.test_table"
        TestView(spark_mock).create_or_replace(select_statement)
        spark_mock.sql.assert_called_once_with(f"CREATE OR REPLACE VIEW test_db.test_view AS ({select_statement})")
