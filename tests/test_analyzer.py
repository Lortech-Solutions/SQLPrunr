import unittest
from sqlprunr.engine.analyzer import analyze_query, find_unused_tables
from sqlprunr.data.generic import Database, Schema, Table, Column
from sqlprunr.data.query_data import Frequencies, QueryData
from sqlprunr.engine.parser import SnowflakeCSVTableParser


class TestAnalyzer(unittest.TestCase):
    def setUp(self):
        self.query = "SELECT column1, column2 FROM table1"
        self.database = Database(
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

    def test_analyze_query(self):
        result = analyze_query(self.query)
        self.assertIn("table1", result["tables"])
        self.assertIn("column1", result["columns"])
        self.assertIn("column2", result["columns"])

    def test_find_unused_tables(self):
        frequencies = Frequencies(
            tables={"table1": 1},
            columns={"column1": 1, "column2": 1},
            queries={"SELECT column1, column2 FROM table1": 1},
        )
        unused_tables = find_unused_tables(frequencies, self.database)
        self.assertEqual(len(unused_tables), 1)
        self.assertEqual(unused_tables[0].name, "table2")


if __name__ == "__main__":
    unittest.main()
