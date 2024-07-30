import typing
import logging
from sql_metadata import Parser
from sqlprunr.data.generic import Database, Table, Column
from sqlprunr.data.query_data import Frequencies, QueryData

def clean_query(query: str) -> str:
    return query.strip().replace("\n", " ")


def analyze_query(query: str, *, execution_time: int = 0) -> dict:
    """
    Analyze the query and return the dimensions

    :param query: Query to analyze
    """
    if query.count(";") > 1:
        raise ValueError("Only one query per input is supported.")

    query = clean_query(query)

    parser = Parser(query, disable_logging=True)

    return {
        "query_ref": query,
        "execution_time": execution_time,
        "tables": parser.tables,
        "columns": parser.columns
    }

def find_unused_tables(frequencies: Frequencies, database: Database) -> typing.List[Table]:
    """
    Find tables that are not used in the queries

    :param queries: List of queries
    :param database: Database schema
    """
    used_tables = set(frequencies.tables.keys())

    unused_tables = []
    for schema in database.schemas:
        for table in schema.tables:
            if table.name not in used_tables:
                logging.warning(f"Found unused table: {database.name}.{schema.name}.{table.name} ({len(table.columns) if table.columns else 0} columns)")
                unused_tables.append(table)

    logging.warning(f"Keep in mind that these tables are not used in specified queries, but they might be used in other.")
    logging.warning(f"Keep in mind that tables were checked only according to the selected database schema, check if specified queries were only executed in selected database area.")

    return unused_tables
