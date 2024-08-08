from sqlprunr.engine.analyzer import analyze_query, find_unused_tables, get_frequencies
from sqlprunr.data.query_data import Frequencies, QueryData


def test_analyze_query(benchmark):
    query = QueryData(
        QUERY_TEXT="SELECT column1, column2 FROM table1",
        START_TIME="2024-06-22 17:17:34.245 +0200",
        END_TIME="2024-06-22 17:17:34.565 +0200",
    )

    result = benchmark(lambda: analyze_query(query))
    assert "table1" in result["tables"]
    assert "column1" in result["columns"]
    assert "column2" in result["columns"]


def test_find_unused_tables(database, benchmark):
    frequencies = Frequencies(
        tables={"table1": 1},
        columns={"column1": 1, "column2": 1},
        queries={"SELECT column1, column2 FROM table1": 1},
    )

    unused_tables = benchmark(lambda: find_unused_tables(frequencies, database))
    assert len(unused_tables) == 1
    assert unused_tables[0].name == "table2"


def test_get_frequencies(query_data, benchmark):
    frequencies = benchmark(lambda: get_frequencies(query_data))
    queries = [query.QUERY_TEXT for query in query_data]

    assert frequencies.tables == {"db1.schema1.table1": 2, "db1.schema1.table2": 1}
    assert frequencies.columns == {"column1": 2, "column2": 1}
    assert frequencies.queries == {
        query.QUERY_TEXT: queries.count(query.QUERY_TEXT) for query in query_data
    }


def test_get_frequencies_no_tables(query_data, benchmark):
    frequencies = benchmark(lambda: get_frequencies(query_data, tables=False))
    queries = [query.QUERY_TEXT for query in query_data]

    assert frequencies.tables == {}
    assert frequencies.columns == {"column1": 2, "column2": 1}
    assert frequencies.queries == {
        query.QUERY_TEXT: queries.count(query.QUERY_TEXT) for query in query_data
    }


def test_get_frequencies_no_columns(query_data, benchmark):
    frequencies = benchmark(lambda: get_frequencies(query_data, columns=False))
    queries = [query.QUERY_TEXT for query in query_data]

    assert frequencies.tables == {"db1.schema1.table1": 2, "db1.schema1.table2": 1}
    assert frequencies.columns == {}
    assert frequencies.queries == {
        query.QUERY_TEXT: queries.count(query.QUERY_TEXT) for query in query_data
    }
