import pytest
from sqlprunr.data.query_data import QueryData
from sqlprunr.data.generic import Database, Schema, Table, Column


@pytest.fixture
def query_data():
    return [
        QueryData(
            QUERY_TEXT="SELECT column1 FROM db1.schema1.table1",
            START_TIME="2021-01-01T00:00:00",
            END_TIME="2021-01-01T00:01:00",
        ),
        QueryData(
            QUERY_TEXT="SELECT column2 FROM db1.schema1.table1",
            START_TIME="2021-01-01T00:01:00",
            END_TIME="2021-01-01T00:02:00",
        ),
        QueryData(
            QUERY_TEXT="SELECT column1 FROM db1.schema1.table2",
            START_TIME="2021-01-01T00:02:00",
            END_TIME="2021-01-01T00:03:00",
        ),
    ]


@pytest.fixture
def database():
    return Database(
        name="db1",
        schemas=[
            Schema(
                name="schema1",
                tables=[
                    Table(
                        name="table1",
                        columns=[
                            Column(name="column1", data_type="TEXT"),
                            Column(name="column2", data_type="NUMBER"),
                        ],
                    ),
                    Table(
                        name="table2",
                        columns=[Column(name="column3", data_type="NUMBER")],
                    ),
                ],
            )
        ],
    )
