from dataclasses import dataclass
from datetime import datetime
import typing

from graphviz import Digraph
from sql_metadata import Parser

from sqlprunr.data.generic import Column, Database, Schema, Table

from abc import ABC, abstractmethod

from sqlprunr.data.query_data import Frequencies, QueryData
from sqlprunr.engine.analyzer import analyze_query


class TableParser(ABC):
    @abstractmethod
    def parse_table(self, query: typing.Any) -> typing.List[Table]:
        """
        Parse abstract query and return a list of tables.

        :param query: Abstract query to parse
        :return: List of tables
        """
        raise NotImplementedError


class SQLTableParser(TableParser):
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


@dataclass
class SnowflakeCSVData:
    """
    SnowflakeCSVData is a dataclass to represent Snowflake database schema from a CSV file.
    """

    DATABASE_NAME: str
    SCHEMA_NAME: str
    TABLE_NAME: str
    COLUMN_NAME: str
    DATA_TYPE: str


class SnowflakeCSVTableParser(TableParser):
    """
    SnowflakeCSVTableParser is a class that parses Snowflake database schema from a CSV file.
    """

    def parse_table(
        self, query: typing.List[SnowflakeCSVData]
    ) -> typing.List[Database]:
        databases = {}
        for data in query:
            database_name = data.DATABASE_NAME
            schema_name = data.SCHEMA_NAME
            table_name = data.TABLE_NAME
            column_name = data.COLUMN_NAME
            data_type = data.DATA_TYPE

            print(
                f"Database: {database_name}, Schema: {schema_name}, Table: {table_name}, Column: {column_name}, Data Type: {data_type}"
            )

            if database_name not in databases:
                print(f"Creating database: {database_name}")
                databases[database_name] = Database(database_name, [])
            database = databases[database_name]

            schema = next((s for s in database.schemas if s.name == schema_name), None)
            if schema is None:
                print(f"Creating schema: {schema_name}")
                schema = Schema(schema_name, [])
                database.schemas.append(schema)

            table = next((t for t in schema.tables if t.name == table_name), None)
            if table is None:
                print(f"Creating table: {table_name}")
                table = Table(table_name, [])
                schema.tables.append(table)

            if not any(c.name == column_name for c in table.columns):
                print(f"Adding column: {column_name}")
                table.columns.append(Column(column_name, data_type))

        return list(databases.values())

    def visualize_structure(self, databases: typing.List[Database]):
        dot = Digraph(comment="Database Schema")

        for db in databases:
            db_id = f"db_{db.name}"
            dot.node(db_id, db.name, shape="box", style="filled", color="lightblue")

            for schema in db.schemas:
                schema_id = f"schema_{db.name}_{schema.name}"
                dot.node(
                    schema_id,
                    schema.name,
                    shape="box",
                    style="filled",
                    color="lightyellow",
                )
                dot.edge(db_id, schema_id)

                for table in schema.tables:
                    table_id = f"table_{db.name}_{schema.name}_{table.name}"
                    dot.node(
                        table_id,
                        table.name,
                        shape="box",
                        style="filled",
                        color="lightgreen",
                    )
                    dot.edge(schema_id, table_id)

                    for column in table.columns:
                        column_id = (
                            f"column_{db.name}_{schema.name}_{table.name}_{column.name}"
                        )
                        dot.node(
                            column_id,
                            f"{column.name}\n({column.data_type})",
                            shape="ellipse",
                        )
                        dot.edge(table_id, column_id)

        return dot

    def get_frequencies(
        self,
        queries: typing.List[QueryData],
        *,
        tables: bool = True,
        columns: bool = True,
    ) -> Frequencies:
        """
        Get frequencies of tables and columns in the queries.

        :param queries: List of queries to analyze
        :param tables: Whether to analyze tables
        :param columns: Whether to analyze columns
        """
        tables = []
        columns = []
        for query in queries:
            try:
                z = analyze_query(
                    query.QUERY_TEXT, execution_time=0
                )
            except (ValueError, IndexError) as e:
                continue

            tables.extend(z["tables"])
            columns.extend(z["columns"])

        frequencies = Frequencies(
            tables={table: tables.count(table) for table in set(tables)},
            columns={column: columns.count(column) for column in set(columns)},
            queries={query.QUERY_TEXT: queries.count(query) for query in set(queries)},
        )

        frequencies.tables = dict(
            sorted(
                frequencies.tables.items(), key=lambda item: item[1], reverse=True
            )
        )
        frequencies.columns = dict(
            sorted(
                frequencies.columns.items(), key=lambda item: item[1], reverse=True
            )
        )

        frequencies.queries = dict(
            sorted(
                frequencies.queries.items(), key=lambda item: item[1], reverse=True
            )
        )

        return frequencies

    def get_time_spent(self, queries: typing.List[QueryData]):
        """
        Get the time spent on each query.

        :param queries: List of queries to analyze
        """
        time_spent = {}

        for query in queries:
            start_time = datetime.fromisoformat(query.START_TIME)
            end_time = datetime.fromisoformat(query.END_TIME)

            time_spent[query.QUERY_TEXT] = (end_time - start_time).total_seconds()

        sorted_time_spent = dict(
            sorted(time_spent.items(), key=lambda item: item[1], reverse=True)
        )

        return sorted_time_spent
