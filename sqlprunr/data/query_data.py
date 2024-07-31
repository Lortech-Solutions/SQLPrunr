from dataclasses import dataclass


@dataclass
class Frequencies:
    tables: dict
    columns: dict
    queries: dict

    def __hash__(self) -> int:
        return hash(self.tables) + hash(self.columns) + hash(self.queries)


@dataclass
class QueryData:
    QUERY_TEXT: str
    START_TIME: str
    END_TIME: str

    def __hash__(self) -> int:
        return hash(self.QUERY_TEXT) + hash(self.START_TIME) + hash(self.END_TIME)
