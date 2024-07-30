from dataclasses import dataclass
from datetime import datetime
import typing
import logging

from sqlprunr.data.generic import Column, Database, Schema, Table

from sqlprunr.data.query_data import Frequencies, QueryData
from sqlprunr.engine.analyzer import analyze_query
from sqlprunr.engine.parser.base import AbstractTableParser


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


class SnowflakeCSVTableParser(AbstractTableParser):
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

            logging.debug(
                f"Database: {database_name}, Schema: {schema_name}, Table: {table_name}, Column: {column_name}, Data Type: {data_type}"
            )

            if database_name not in databases:
                logging.debug(f"Creating database: {database_name}")
                databases[database_name] = Database(database_name, [])
            database = databases[database_name]

            schema = next((s for s in database.schemas if s.name == schema_name), None)
            if schema is None:
                logging.debug(f"Creating schema: {schema_name}")
                schema = Schema(schema_name, [])
                database.schemas.append(schema)

            table = next((t for t in schema.tables if t.name == table_name), None)
            if table is None:
                logging.debug(f"Creating table: {table_name}")
                table = Table(table_name, [])
                schema.tables.append(table)

            if not any(c.name == column_name for c in table.columns):
                logging.debug(f"Adding column: {column_name}")
                table.columns.append(Column(column_name, data_type))

        return list(databases.values())
