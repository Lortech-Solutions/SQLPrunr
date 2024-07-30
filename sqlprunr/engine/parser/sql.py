import typing

from sql_metadata import Parser

from sqlprunr.data.generic import Column, Table
from sqlprunr.engine.parser.base import AbstractTableParser


class SQLTableParser(AbstractTableParser):
    """
    SQLTableParser is a class that parses SQL database schema from a SQL query.
    """

    def parse_table(self, query: str) -> typing.List[Table]:
        queries = query.split(";")

        result = []
        for query in queries:
            parser = Parser(query)
            tables = parser.tables

            for table in tables:
                columns = parser.columns
                table_name = table.split(" ")[-1]
                table_columns = [Column(column) for column in columns]
                result.append(Table(table_name, table_columns))

        return result