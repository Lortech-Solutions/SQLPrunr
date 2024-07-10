from dataclasses import dataclass
import typing

from sqlprunr.data.generic import Column, Table


@dataclass
class Dimension:
    """
    Represents a dimension in a query.

    Columns in Dimension reference to columns in Table.
    Keep in mind these are not all columns in the original table,
    but only those that are used in the query.
    """
    table: Table
    used_columns: typing.List[Column]
