from sqlprunr.engine.parser import parse_table
from tests.data import schema


single_table_schema = """
CREATE TABLE Customers (
    CustomerID INT PRIMARY KEY,
    CustomerName VARCHAR(100),
    ContactName VARCHAR(100),
    Country VARCHAR(50),
    UnusedColumn1 VARCHAR(100)
);
"""


def test_parse_single_table():
    table = parse_table(single_table_schema)[0]
    assert table.name == 'Customers'
    assert len(table.columns) == 5


def test_parse_multiple_tables():
    table = parse_table(schema)
    assert len(table) == 8
    assert table[0].name == 'Customers'
    assert table[1].name == 'Orders'
    assert table[2].name == 'OrderDetails'
    assert table[3].name == 'Products'
    assert table[4].name == 'Suppliers'
    assert table[5].name == 'Categories'
    assert table[6].name == 'Shippers'
    assert table[7].name == 'UnusedTable'
    assert len(table[0].columns) == 5
    assert len(table[1].columns) == 5
    assert len(table[2].columns) == 5
    assert len(table[3].columns) == 6
    assert len(table[4].columns) == 5
    assert len(table[5].columns) == 4
    assert len(table[6].columns) == 4
    assert len(table[7].columns) == 1
    assert table[1].columns[1].name == 'CustomerID'
    assert table[1].columns[2].name == 'OrderDate'
    assert table[1].columns[3].name == 'ShipperID'
    assert table[1].columns[4].name == 'UnusedColumn2'
    assert table[1].columns[0].name == 'OrderID'
    assert table[2].columns[1].name == 'OrderID'
    assert table[2].columns[2].name == 'ProductID'
    assert table[2].columns[3].name == 'Quantity'
    assert table[2].columns[4].name == 'UnusedColumn3'
    assert table[2].columns[0].name == 'OrderDetailID'
    assert table[3].columns[1].name == 'ProductName'
    assert table[3].columns[2].name == 'SupplierID'
    assert table[3].columns[3].name == 'CategoryID'
    assert table[3].columns[4].name == 'Price'
    assert table[3].columns[5].name == 'UnusedColumn4'
    assert table[3].columns[0].name == 'ProductID'
    assert table[4].columns[1].name == 'SupplierName'
    assert table[4].columns[2].name == 'ContactName'
    assert table[4].columns[3].name == 'Country'
    assert table[4].columns[4].name == 'UnusedColumn5'
    assert table[4].columns[0].name == 'SupplierID'
    assert table[5].columns[1].name == 'CategoryName'
    assert table[5].columns[2].name == 'Description'
    assert table[5].columns[3].name == 'UnusedColumn6'
    assert table[5].columns[0].name == 'CategoryID'
    assert table[6].columns[1].name == 'ShipperName'
    assert table[6].columns[2].name == 'Phone'
    assert table[6].columns[3].name == 'UnusedColumn7'
    assert table[7].columns[0].name == 'UnusedColumn'
