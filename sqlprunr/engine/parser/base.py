import typing
from abc import ABC, abstractmethod

from sqlprunr.data.generic import Table


class AbstractTableParser(ABC):
    @abstractmethod
    def parse_table(self, query: typing.Any) -> typing.List[Table]:
        """
        Parse abstract query and return a list of tables.

        :param query: Abstract query to parse
        :return: List of tables
        """
        raise NotImplementedError
