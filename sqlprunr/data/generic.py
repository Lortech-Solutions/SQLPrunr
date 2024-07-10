from dataclasses import dataclass
import typing


@dataclass(frozen=True)
class Column:
    name: str

    def __hash__(self):
        return hash(self.name)


@dataclass(frozen=True)
class Table:
    name: str
    columns: typing.List[Column]

    def __hash__(self):
        return hash(self.name)
