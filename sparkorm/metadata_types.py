from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import NamedTuple, Optional, Type, Union

from pyspark.sql import SparkSession
from strenum import StrEnum


class DBConfig(ABC):
    @classmethod
    @abstractmethod
    def get_name(cls) -> str:
        raise NotImplementedError()

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def ensure_exists(self, location: Optional[str] = None) -> None:
        if location:
            self._spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.get_name()} LOCATION '{location}'")
            return
        self._spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.get_name()}")

    def drop(self) -> None:
        self._spark.sql(f"DROP DATABASE IF EXISTS {self.get_name()} CASCADE")


class SchemaMigrationStrategy(ABC):
    """
    Base class for schema migration strategies
    """


class DropAndCreateStrategy(SchemaMigrationStrategy):
    """Drop the table and create it again"""


class AddAndDropStrategy(SchemaMigrationStrategy):
    """Add new columns to the table, and drop columns that are not in the model"""


class AddOnlyStrategy(SchemaMigrationStrategy):
    """Add new columns to the table, but do not drop columns that are not in the model"""


class NoChangeStrategy(SchemaMigrationStrategy):
    """Do not change the table schema"""


class SchemaUpdateStatus(Enum):
    """
    Status of the schema update
    """

    CREATED = auto()
    SKIPPED = auto()
    DROPPED_AND_CREATED = auto()
    REPLACED = auto()


class LocationType(StrEnum):
    TEXT = auto()
    AVRO = auto()
    BINARYFILE = auto()
    CSV = auto()
    JSON = auto()
    PARQUET = auto()
    ORC = auto()
    JDBC = auto()
    DELTA = auto()
    LIBSVM = auto()


class LocationConfig(NamedTuple):
    type: LocationType
    location: str


class MetaConfig(ABC):
    migration_strategy: SchemaMigrationStrategy = NoChangeStrategy()
    db_config: Optional[Union[Type[DBConfig], DBConfig]] = None
    name: str
    location: Optional[LocationConfig] = None

    @classmethod
    def get_name(cls) -> str:
        return cls.name

    @classmethod
    def get_db_name(cls) -> Optional[str]:
        if cls.db_config is not None:
            assert issubclass(cls.db_config, DBConfig) or isinstance(cls.db_config, DBConfig), str(cls.db_config.__class__)
            return cls.db_config.get_name()
        return None

    @classmethod
    def get_migration_strategy(cls) -> SchemaMigrationStrategy:
        assert isinstance(cls.migration_strategy, SchemaMigrationStrategy)
        return cls.migration_strategy

    @classmethod
    def get_location(cls) -> Optional[LocationConfig]:
        return cls.location
