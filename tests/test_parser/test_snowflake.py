import pytest
from sqlprunr.engine.parser.snowflake import SnowflakeCSVData, SnowflakeCSVTableParser

@pytest.fixture
def parser():
    return SnowflakeCSVTableParser()


@pytest.fixture
def csv_data():
    return [
        SnowflakeCSVData(
            DATABASE_NAME="db1",
            SCHEMA_NAME="schema1",
            TABLE_NAME="table1",
            COLUMN_NAME="column1",
            DATA_TYPE="STRING"
        ),
        SnowflakeCSVData(
            DATABASE_NAME="db1",
            SCHEMA_NAME="schema1",
            TABLE_NAME="table1",
            COLUMN_NAME="column2",
            DATA_TYPE="INT"
        ),
        SnowflakeCSVData(
            DATABASE_NAME="db1",
            SCHEMA_NAME="schema2",
            TABLE_NAME="table2",
            COLUMN_NAME="column1",
            DATA_TYPE="FLOAT"
        ),
    ]

def test_snowflake_parse_table(parser, csv_data, benchmark):
    def parse_data():
        return parser.parse_table(csv_data)
    
    databases = benchmark(parse_data)
    
    assert len(databases) == 1
    assert databases[0].name == "db1"
    assert len(databases[0].schemas) == 2

    schema1 = databases[0].schemas[0]
    assert schema1.name == "schema1"
    assert len(schema1.tables) == 1
    table1 = schema1.tables[0]
    assert table1.name == "table1"
    assert len(table1.columns) == 2
    assert table1.columns[0].name == "column1"
    assert table1.columns[0].data_type == "STRING"
    assert table1.columns[1].name == "column2"
    assert table1.columns[1].data_type == "INT"

    schema2 = databases[0].schemas[1]
    assert schema2.name == "schema2"
    assert len
