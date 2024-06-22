import os
import tempfile

USE_DATABRICKS = int(os.getenv("USE_DATABRICKS", "0")) == 1

if USE_DATABRICKS:
    from databricks.connect import DatabricksSession as SparkSession
else:
    from pyspark.sql import SparkSession


class SparkConnector:
    """
    Singleton class to get the SparkSession object
    """

    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if USE_DATABRICKS:
            self.spark = self._dbx_conn()
        else:
            self.spark = self._spark_conn()

    @staticmethod
    def _dbx_conn() -> SparkSession:
        spark = SparkSession.builder.remote(host="", token="", cluster_id="").getOrCreate()
        return spark

    @staticmethod
    def _spark_conn() -> SparkSession:
        hive_store_dir = tempfile.mkdtemp(prefix="spark-warehouse-")
        spark = (
            SparkSession.builder.appName("test")
            .master("local[*]")  # Use all available cores
            .config("spark.sql.catalogImplementation", "in-memory")
            .config("spark.sql.warehouse.dir", hive_store_dir)  # Hive warehouse directory
            .config("spark.sql.catalogImplementation", "hive")  # Use Hive catalog
            .config("spark.sql.hive.convertMetastoreParquet", "false")  # Use Hive SerDe for Parquet
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  # Use Kryo serialization
            .config("spark.sql.execution.arrow.enabled", "true")  # Enable Arrow for faster Pandas conversion
            .config("spark.executor.memory", "4g")  # Executor memory
            .config("spark.driver.memory", "4g")  # Driver memory
            .config("spark.executor.cores", "4")  # Number of cores per executor
            .config("spark.default.parallelism", "2")
            .config("spark.sql.shuffle.partitions", "5")  # Reduce number of shuffle partitions
            .config("spark.sql.session.timeZone", "UTC")  # Set timezone to UTC
            .config("javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={hive_store_dir}/metastore_db;create=true")
            .enableHiveSupport()  # Enable Hive support
            .getOrCreate()
        )
        return spark

    def get(self) -> SparkSession:
        return self.spark
