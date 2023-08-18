from abc import ABC, abstractmethod
from enum import auto, Enum
from typing import Optional, Type, Union


class DBConfig(ABC):
    @classmethod
    @abstractmethod
    def get_name(cls) -> str:
        ...


class SchemaMigrationStrategy(ABC):
    """
    Base class for schema migration strategies
    """


class DropAndCreateStrategy(SchemaMigrationStrategy):
    """ Drop the table and create it again """


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


class MetaConfig(ABC):
    migration_strategy: SchemaMigrationStrategy = NoChangeStrategy()
    db_config: Optional[Union[Type[DBConfig], DBConfig]] = None
    name: str

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
    def _get_migration_strategy(cls) -> SchemaMigrationStrategy:
        assert isinstance(cls.migration_strategy, SchemaMigrationStrategy)
        return cls.migration_strategy
