import csv
from dataclasses import dataclass
import io
from sqlprunr.data.query_data import QueryData
from sqlprunr.engine.parser import SnowflakeCSVData, SnowflakeCSVTableParser
from sqlprunr.engine.analyzer import analyze_query, find_unused_tables
from functools import lru_cache

import seaborn as sns
import matplotlib.pyplot as plt

from sqlprunr.engine.visualizer import visualize_frequencies

parser = SnowflakeCSVTableParser()

@lru_cache
def get_data():
    with open("_rtests/schema_with_columns.csv", "r") as f:
        data = f.read()
        reader = csv.DictReader(io.StringIO(data.strip()))

    data = [SnowflakeCSVData(**row) for row in reader]
    databases = parser.parse_table(data)
    return databases


def test_db_struct_visalization():
    databases = get_data()

    databases[0].schemas = databases[0].schemas[:2]
    databases[0].schemas[0].tables = databases[0].schemas[0].tables[:2]
    databases[0].schemas[0].tables[0].columns = (
        databases[0].schemas[0].tables[0].columns[:5]
    )
    databases[0].schemas[0].tables[1].columns = (
        databases[0].schemas[0].tables[1].columns[:5]
    )
    databases[0].schemas[1].tables = databases[0].schemas[1].tables[:2]
    databases[0].schemas[1].tables[0].columns = (
        databases[0].schemas[1].tables[0].columns[:5]
    )
    databases[0].schemas[1].tables[1].columns = (
        databases[0].schemas[1].tables[1].columns[:5]
    )
    dot = parser.visualize_structure(databases)
    dot.render("database_structure", format="png", view=True)


def analyze():
    databases = get_data()
    
    with open("_rtests/query_list.csv", "r") as f:
        query = f.read()

    reader = csv.DictReader(io.StringIO(query.strip()))
    data = [QueryData(**row) for row in reader]

    frequencies = parser.get_frequencies(data)

    # visualize_frequencies(frequencies, savefig=True)

    unused_tables = find_unused_tables(frequencies, databases[0])
    print(len(unused_tables))

if __name__ == "__main__":
    analyze()
