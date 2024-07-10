import typing
from sql_metadata import Parser

from sqlprunr.data.generic import Column, Table


def parse_table(sql_query: str) -> typing.List[Table]:
    queries = sql_query.split(";")

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
