from dataclasses import dataclass
import typing

from sqlprunr.data.dimension import Dimension


@dataclass
class QueryData:
    """
    Represents a query with its dimensions.
    """
    query: str
    dimensions: typing.List[Dimension]
