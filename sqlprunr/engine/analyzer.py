from datetime import datetime
import typing
import logging
from sql_metadata import Parser

from sqlprunr.data.generic import Database, Table
from sqlprunr.data.query_data import Frequencies, QueryData

# from functools import lru_cache


def clean_query(query: str) -> str:
    return query.strip().replace("\n", " ")


# @lru_cache()
def analyze_query(query_data: QueryData, *, execution_time: int = 0) -> dict:
    """
    Analyze the query and return the dimensions

    :param query: Query to analyze
    """
    query = query_data.QUERY_TEXT
    if query.count(";") > 1:
        raise ValueError("Only one query per input is supported.")

    query = clean_query(query)

    parser = Parser(query, disable_logging=True)

    return {
        "query_ref": query,
        "execution_time": execution_time,
        "tables": parser.tables,
        "columns": parser.columns,
    }


def find_unused_tables(
    frequencies: Frequencies, database: Database
) -> typing.List[Table]:
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
                logging.debug(
                    f"Found unused table: {database.name}.{schema.name}.{table.name} ({len(table.columns) if table.columns else 0} columns)"
                )
                unused_tables.append(table)

    logging.warning(
        "Keep in mind that these tables are not used in specified queries, but they might be used in other."
    )
    logging.warning(
        "Keep in mind that tables were checked only according to the selected database schema, check if specified queries were only executed in selected database area."
    )

    return unused_tables


def get_frequencies(
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
    table_list = []
    column_list = []
    for query in queries:
        try:
            z = analyze_query(query, execution_time=0)
        except (ValueError, IndexError):
            continue

        if tables:
            table_list.extend(z["tables"])
        if columns:
            column_list.extend(z["columns"])

    frequencies = Frequencies(
        tables={table: table_list.count(table) for table in set(table_list)},
        columns={column: column_list.count(column) for column in set(column_list)},
        queries={query.QUERY_TEXT: queries.count(query) for query in set(queries)},
    )

    frequencies.tables = dict(
        sorted(frequencies.tables.items(), key=lambda item: item[1], reverse=True)
    )
    frequencies.columns = dict(
        sorted(frequencies.columns.items(), key=lambda item: item[1], reverse=True)
    )

    frequencies.queries = dict(
        sorted(frequencies.queries.items(), key=lambda item: item[1], reverse=True)
    )

    return frequencies


def get_time_spent(queries: typing.List[QueryData]):
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
