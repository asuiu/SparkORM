from abc import ABC, abstractmethod


class DBConfig(ABC):
    @classmethod
    @abstractmethod
    def get_name(cls) -> str:
        ...
