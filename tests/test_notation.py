import csv
import io
import typing

import pytest
from sqlprunr.data.query_data import QueryData
from sqlprunr.engine.analyzer import analyze_query
import big_o


def gen_query_data(n: int) -> typing.List[QueryData]:
    columns = [f"column{i}" for i in range(n)]
    return f"SELECT {', '.join(columns)} FROM db1.schema1.table1"


@pytest.fixture
def rtest_queries_data():
    with open("_rtests/query_list.csv", "r") as f:
        query = f.read()

    reader = csv.DictReader(io.StringIO(query.strip()))
    data = [row['QUERY_TEXT'] for row in reader]
    
    return data

def use_real_data(n: int, data):
    return data[n]


def analyze_query_wrapper(query: str):
    try:
        return analyze_query(query)
    except (ValueError, IndexError):
        return None

def test_analyze_queries_notation(rtest_queries_data, capsys):
    best, others = big_o.big_o(
        analyze_query_wrapper, lambda n: use_real_data(n, rtest_queries_data), min_n=1, max_n=len(rtest_queries_data)-1
    )
    with capsys.disabled():
        print(f"\nAnalyze queries report ({len(rtest_queries_data)} rows):\n{'-'*30}\n" + big_o.reports.big_o_report(best, others))

    MAX_N = 100
    best, others = big_o.big_o(
        analyze_query_wrapper, gen_query_data, min_n=1, max_n=MAX_N
    )
    with capsys.disabled():
        print(f"\nAnalyze queries report ({MAX_N} dummy rows [adding column to query every n]):\n{'-'*30}\n" + big_o.reports.big_o_report(best, others))
