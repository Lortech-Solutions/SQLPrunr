import typing
from sql_metadata import Parser
from sqlprunr.data.dimension import Dimension
from sqlprunr.data.generic import Table, Column

from sqlprunr.data.query_data import QueryData


def clean_query(query: str) -> str:
    return query.strip().replace("\n", " ")


def analyze_query(query: str, orginal_schema: typing.Optional[typing.List[Table]] = None):
    if query.count(";") > 1:
        raise ValueError("Only one query per input is supported.")

    query = clean_query(query)

    parser = Parser(query)

    tables_mapping = {}
    for column in parser.columns:
        table_name, column_name = column.split(".")
        tables_mapping[table_name] = tables_mapping.get(table_name, []) + [column_name]

    dimensions = []

    for table_name, columns in tables_mapping.items():
        columns = [Column(name=column) for column in columns]

        table = [table for table in orginal_schema if table.name == table_name][0] if orginal_schema else Table(name=table_name, columns=columns)
        # logging.warning("Orginal schema not provided, Table in Dimension will have the same columns as in the query. (No analytics possible)")

        dimensions.append(Dimension(table=table, used_columns=columns))

    sorted_dimensions = sorted(dimensions, key=lambda x: x.table.name)

    return QueryData(
        query=query,
        dimensions=sorted_dimensions
    )
