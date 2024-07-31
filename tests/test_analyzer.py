from sqlprunr.engine.analyzer import analyze_query, find_unused_tables
from sqlprunr.data.query_data import Frequencies, QueryData


def test_analyze_query(benchmark):
    query = QueryData(
        QUERY_TEXT="SELECT column1, column2 FROM table1",
        START_TIME="2024-06-22 17:17:34.245 +0200",
        END_TIME="2024-06-22 17:17:34.565 +0200",
    )

    def analyze():
        return analyze_query(query)

    result = benchmark(analyze)
    assert "table1" in result["tables"]
    assert "column1" in result["columns"]
    assert "column2" in result["columns"]


def test_find_unused_tables(database, benchmark):
    frequencies = Frequencies(
        tables={"table1": 1},
        columns={"column1": 1, "column2": 1},
        queries={"SELECT column1, column2 FROM table1": 1},
    )

    def find_tables():
        return find_unused_tables(frequencies, database)

    unused_tables = benchmark(find_tables)
    assert len(unused_tables) == 1
    assert unused_tables[0].name == "table2"
