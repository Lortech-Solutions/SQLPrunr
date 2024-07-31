from dataclasses import asdict


def test_query_data_serialization(query_data):
    serialized_query_data = [asdict(query) for query in query_data]

    assert serialized_query_data == [
        {
            "QUERY_TEXT": "SELECT column1 FROM db1.schema1.table1",
            "START_TIME": "2021-01-01T00:00:00",
            "END_TIME": "2021-01-01T00:01:00",
        },
        {
            "QUERY_TEXT": "SELECT column2 FROM db1.schema1.table1",
            "START_TIME": "2021-01-01T00:01:00",
            "END_TIME": "2021-01-01T00:02:00",
        },
        {
            "QUERY_TEXT": "SELECT column1 FROM db1.schema1.table2",
            "START_TIME": "2021-01-01T00:02:00",
            "END_TIME": "2021-01-01T00:03:00",
        },
    ]
