from decimal import Decimal as D
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.catalog import Column
from pyspark.sql.types import Row

from sparkorm import Decimal, String, Array, Map
from sparkorm.models import TableModel
from sparkorm.utils import create_model_code
from tests.unit.test_models import TestPartitionedTable
from tests.utilities import convert_to_spark_types


class TestCreateModelCode:

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

    def test_create_model_code_mocked(self):
        mock_columns = [Column(name='vendor_key', description=None, dataType='string', nullable=True, isPartition=False, isBucket=False),
                        Column(name='invoice_date', description=None, dataType='timestamp', nullable=True, isPartition=False, isBucket=False),
                        Column(name='amt', description=None, dataType='decimal(18,3)', nullable=False, isPartition=False, isBucket=False),
                        Column(name='current_date', description=None, dataType='date', nullable=False, isPartition=True, isBucket=False)]
        spark_mock = MagicMock(spec=SparkSession)
        spark_mock.catalog.listColumns.return_value = mock_columns
        model_str = create_model_code(spark_mock, TestPartitionedTable.get_db_name(), table_name=TestPartitionedTable.get_name(),
                                      db_config_map={TestPartitionedTable.get_db_name(): None})
        expected_class_repr = '''
class TestPTable(TableModel):
   class Meta:
       name = "test_p_table"

   vendor_key = String()
   invoice_date = Timestamp()
   amt = Decimal(precision=18, scale=3, nullable=False)
   current_date = Date(nullable=False, partitioned_by=True)
'''
        assert model_str == expected_class_repr

    def test_create_model_code(self, setup_clean_spark_catalog, spark_session: SparkSession):
        """
        We expect the create method to raise an error if the table exists with a different schema.
        In our case we'll pass the same schema, but because we can't create real tables in the test environment, there will be view, which is not partitioned,
            and the lack of partitioned fieldswill cause the error.
        """

        class TestTableWithMap(TableModel):
            class Meta:
                name = "test_p_table"

            map_field = Map(String(), Array(Array(String())))
            dec_field = Decimal(18, 3)

        # spark_schema = schema(TestTableWithMap)
        # MockStructRow = Row("string_field", "float_field")
        # TestTableRow = Row(*TestTableWithMap.get_spark_schema().names)
        row1 = Row(
            map_field={
                "key1": [["value1_1_1", "value1_1_2"], ["value1_2_1"]],
                "key2": [["value1_3_1"]]
            },
            dec_field=D("123.456")
        )
        row2 = Row(
            map_field={
                "keyA": [["value2_1_1"], ["value2_2_1", "value2_2_2"]],
                "keyB": [["value2_3_1", "value2_3_2"]]
            },
            dec_field=D("789.012")
        )

        data = [row1, row2]
        spark_struct_type = TestTableWithMap.get_spark_schema()
        rows = convert_to_spark_types(data, spark_struct_type)
        df = spark_session.createDataFrame(rows, schema=spark_struct_type)
        df.createOrReplaceTempView(TestTableWithMap.get_full_name())
        model_str = create_model_code(spark_session, TestTableWithMap.get_db_name(), table_name=TestTableWithMap.get_name(),
                                      db_config_map={TestTableWithMap.get_db_name(): None})
        expected_class_repr = '''
class TestPTable(TableModel):
   class Meta:
       name = "test_p_table"

   map_field = Map(key=String(), value=Array(element=Array(element=String())))
   dec_field = Decimal(precision=18, scale=3)
'''
        assert model_str == expected_class_repr