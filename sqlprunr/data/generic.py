from dataclasses import dataclass
import typing


@dataclass
class Database:
    """
    Database dataclass to represent a database

    :param name: Name of the database
    :param tables: List of tables in the database
    """
    name: str
    schemas: typing.List["Schema"]

    def __hash__(self):
        return hash(self.name)


@dataclass
class Schema:
    """
    Column dataclass to represent a column in a table

    :param name: Name of the column
    """
    name: str
    tables: typing.List["Table"]

    def __hash__(self):
        return hash(self.name)


@dataclass
class Table:
    """
    Table dataclass to represent a table in a database

    :param name: Name of the table
    :param columns: List of columns in the table
    """
    name: str
    columns: typing.List["Column"]

    def __hash__(self):
        return hash(self.name)


@dataclass(frozen=True)
class Column:
    """
    Column dataclass to represent a column in a table

    :param name: Name of the column
    """
    name: str
    data_type: str

    def __hash__(self):
        return hash(self.name)