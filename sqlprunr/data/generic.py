from dataclasses import dataclass
import typing


@dataclass(frozen=True)
class Column:
    """
    Column dataclass to represent a column in a table

    :param name: Name of the column
    """
    name: str

    def __hash__(self):
        return hash(self.name)


@dataclass(frozen=True)
class Table:
    """
    Table dataclass to represent a table in a database

    :param name: Name of the table
    :param columns: List of columns in the table
    """
    name: str
    columns: typing.List[Column]

    def __hash__(self):
        return hash(self.name)
