import csv
import io
import os
import typing
import pytest
import big_o

from sqlprunr.engine.parser.snowflake import SnowflakeCSVData, SnowflakeCSVTableParser


@pytest.fixture
def parser():
    return SnowflakeCSVTableParser()


def get_csv_data(n: int) -> typing.List[SnowflakeCSVData]:
    return [
        SnowflakeCSVData(
            DATABASE_NAME="db",
            SCHEMA_NAME="schema1",
            TABLE_NAME=f"table{n}",
            COLUMN_NAME=f"column{n}",
            DATA_TYPE="INT",
        ) for n in range(n)
    ]


@pytest.fixture
def rtest_database():
    with open("_rtests/schema_with_columns.csv", "r") as f:
        query = f.read()

    reader = csv.DictReader(io.StringIO(query.strip()))
    data = [SnowflakeCSVData(**row) for row in reader]

    return data


@pytest.mark.skipif(os.getenv("CI", False),
                    reason="Cannot run on CI/CD due to the use of prohibited real data in a test case.")
def test_snowflake_parser_notation(parser, rtest_database, capsys):
    def data_generator(n):
        return rtest_database[:n]

    best, others = big_o.big_o(
        parser.parse_table,
        data_generator,
        min_n=1,
        max_n=2,
    )
    with capsys.disabled():
        print(
            f"\nSnowflake parser report ({len(rtest_database)} rows):\n{'-'*30}\n"
            + big_o.reports.big_o_report(best, others)
        )
