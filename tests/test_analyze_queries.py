import pytest
from sqlprunr.data.query_data import QueryData
from sqlprunr.data.dimension import Dimension

from sqlprunr.engine.analyzer import analyze_query, clean_query
from sqlprunr.engine.parser import parse_table

from tests.data import schema


queries = [
    """
    SELECT c.CustomerName, o.OrderDate, SUM(od.Quantity * p.Price) AS Total
    FROM Customers c
    JOIN Orders o ON c.CustomerID = o.CustomerID
    JOIN OrderDetails od ON o.OrderID = od.OrderID
    JOIN Products p ON od.ProductID = p.ProductID
    GROUP BY c.CustomerName, o.OrderDate
    HAVING SUM(od.Quantity * p.Price) > 100
    ORDER BY Total DESC
    """,
    """
    SELECT s.SupplierName, COUNT(p.ProductID) AS ProductCount, AVG(p.Price) AS AveragePrice
    FROM Suppliers s
    JOIN Products p ON s.SupplierID = p.SupplierID
    GROUP BY s.SupplierName
    HAVING AVG(p.Price) > 50
    ORDER BY ProductCount DESC
    """,
    """
    SELECT c.CategoryName, COUNT(p.ProductID) AS NumberOfProducts, MAX(p.Price) AS MaxPrice
    FROM Categories c
    JOIN Products p ON c.CategoryID = p.CategoryID
    GROUP BY c.CategoryName
    ORDER BY MaxPrice DESC
    """,
    """
    SELECT o.OrderID, c.CustomerName, s.ShipperName, o.OrderDate
    FROM Orders o
    JOIN Customers c ON o.CustomerID = c.CustomerID
    JOIN Shippers s ON o.ShipperID = s.ShipperID
    WHERE o.OrderDate BETWEEN '2023-01-01' AND '2023-12-31'
    ORDER BY o.OrderDate
    """,
    """
    SELECT p.ProductName, SUM(od.Quantity) AS TotalQuantitySold
    FROM OrderDetails od
    JOIN Products p ON od.ProductID = p.ProductID
    GROUP BY p.ProductName
    HAVING SUM(od.Quantity) > 100
    ORDER BY TotalQuantitySold DESC
    """,
    """
    SELECT c.CustomerName, COUNT(o.OrderID) AS NumberOfOrders
    FROM Customers c
    JOIN Orders o ON c.CustomerID = o.CustomerID
    GROUP BY c.CustomerName
    ORDER BY NumberOfOrders DESC
    LIMIT 10
    """,
    """
    SELECT p.ProductName, c.CategoryName, s.SupplierName, p.Price
    FROM Products p
    JOIN Categories c ON p.CategoryID = c.CategoryID
    JOIN Suppliers s ON p.SupplierID = s.SupplierID
    ORDER BY p.Price DESC
    """,
    """
    SELECT c.Country, COUNT(DISTINCT c.CustomerID) AS NumberOfCustomers
    FROM Customers c
    GROUP BY c.Country
    HAVING COUNT(DISTINCT c.CustomerID) > 5
    ORDER BY NumberOfCustomers DESC
    """,
    """
    SELECT o.OrderID, o.OrderDate, SUM(od.Quantity * p.Price) AS OrderTotal
    FROM Orders o
    JOIN OrderDetails od ON o.OrderID = od.OrderID
    JOIN Products p ON od.ProductID = p.ProductID
    GROUP BY o.OrderID, o.OrderDate
    ORDER BY OrderTotal DESC
    LIMIT 20
    """,
    """
    SELECT s.SupplierName, c.CategoryName, COUNT(p.ProductID) AS NumberOfProducts
    FROM Suppliers s
    JOIN Products p ON s.SupplierID = p.SupplierID
    JOIN Categories c ON p.CategoryID = c.CategoryID
    GROUP BY s.SupplierName, c.CategoryName
    ORDER BY NumberOfProducts DESC
    """,
    """
    SELECT c.CustomerName, o.OrderDate, p.ProductName, od.Quantity, (od.Quantity * p.Price) AS LineTotal
    FROM Customers c
    JOIN Orders o ON c.CustomerID = o.CustomerID
    JOIN OrderDetails od ON o.OrderID = od.OrderID
    JOIN Products p ON od.ProductID = p.ProductID
    ORDER BY o.OrderDate, c.CustomerName
    """,
    """
    SELECT c.CategoryName, SUM(od.Quantity) AS TotalQuantitySold
    FROM Categories c
    JOIN Products p ON c.CategoryID = p.CategoryID
    JOIN OrderDetails od ON p.ProductID = od.ProductID
    GROUP BY c.CategoryName
    ORDER BY TotalQuantitySold DESC
    """,
    """
    SELECT o.OrderID, COUNT(od.OrderDetailID) AS NumberOfItems, SUM(od.Quantity) AS TotalQuantity
    FROM Orders o
    JOIN OrderDetails od ON o.OrderID = od.OrderID
    GROUP BY o.OrderID
    HAVING SUM(od.Quantity) > 10
    ORDER BY TotalQuantity DESC
    """,
    """
    SELECT c.CustomerName, o.OrderDate, SUM(od.Quantity * p.Price) AS OrderValue
    FROM Customers c
    JOIN Orders o ON c.CustomerID = o.CustomerID
    JOIN OrderDetails od ON o.OrderID = od.OrderID
    JOIN Products p ON od.ProductID = p.ProductID
    WHERE o.OrderDate > '2023-01-01'
    GROUP BY c.CustomerName, o.OrderDate
    ORDER BY OrderValue DESC
    """,
    """
    SELECT s.ShipperName, COUNT(o.OrderID) AS NumberOfOrders, SUM(od.Quantity) AS TotalQuantityShipped
    FROM Shippers s
    JOIN Orders o ON s.ShipperID = o.ShipperID
    JOIN OrderDetails od ON o.OrderID = od.OrderID
    GROUP BY s.ShipperName
    ORDER BY NumberOfOrders DESC
    """,
    """
    SELECT p.ProductName, s.SupplierName, p.Price
    FROM Products p
    JOIN Suppliers s ON p.SupplierID = s.SupplierID
    WHERE p.Price > (SELECT AVG(p2.Price) FROM Products p2)
    ORDER BY p.Price DESC
    """,
    """
    SELECT c.CustomerName, s.SupplierName, COUNT(o.OrderID) AS NumberOfOrders
    FROM Customers c
    JOIN Orders o ON c.CustomerID = o.CustomerID
    JOIN OrderDetails od ON o.OrderID = od.OrderID
    JOIN Products p ON od.ProductID = p.ProductID
    JOIN Suppliers s ON p.SupplierID = s.SupplierID
    GROUP BY c.CustomerName, s.SupplierName
    ORDER BY NumberOfOrders DESC
    """,
    """
    SELECT c.CategoryName, p.ProductName, COUNT(od.OrderDetailID) AS NumberOfOrders
    FROM Categories c
    JOIN Products p ON c.CategoryID = p.CategoryID
    JOIN OrderDetails od ON p.ProductID = od.ProductID
    GROUP BY c.CategoryName, p.ProductName
    ORDER BY NumberOfOrders DESC
    """,
    """
    SELECT o.OrderDate, COUNT(o.OrderID) AS NumberOfOrders, SUM(od.Quantity) AS TotalQuantity
    FROM Orders o
    JOIN OrderDetails od ON o.OrderID = od.OrderID
    GROUP BY o.OrderDate
    ORDER BY o.OrderDate
    """,
    """
    SELECT s.SupplierName, c.CategoryName, AVG(p.Price) AS AveragePrice
    FROM Suppliers s
    JOIN Products p ON s.SupplierID = p.SupplierID
    JOIN Categories c ON p.CategoryID = c.CategoryID
    GROUP BY s.SupplierName, c.CategoryName
    ORDER BY AveragePrice DESC
    """,
]


def test_analyze_single_query():
    """
    Test single query with hard coded asserts to check if the output is correct
    """
    query = queries[0]
    result = analyze_query(query, orginal_schema=parse_table(schema))

    assert isinstance(result, QueryData)
    assert isinstance(result.dimensions, list)
    assert all(isinstance(dimension, Dimension) for dimension in result.dimensions)

    assert result.query == clean_query(query)
    assert len(result.dimensions) > 0

    USED_TABLES = ("Customers", "OrderDetails", "Orders", "Products")
    USED_COLUMNS = ("CustomerName", "OrderDate", "Quantity", "Price", "OrderID", "ProductID", "CustomerID", "Total")
    # Below is the output of the test, it is not the same as the USED_COLUMNS above because the columns are duplicated in the schema
    # This is because multiple columns have the same name but are in different tables
    # ['CustomerName', 'CustomerID', 'Quantity', 'OrderID', 'ProductID', 'OrderDate', 'CustomerID', 'OrderID', 'Price', 'ProductID']

    assert all(dimension.table.name in USED_TABLES for dimension in result.dimensions)
    assert all(column.name in USED_COLUMNS for dimension in result.dimensions for column in dimension.used_columns)


@pytest.mark.parametrize("query", queries)
def test_analyze_query(query):
    result = analyze_query(query, orginal_schema=parse_table(schema))

    assert isinstance(result, QueryData)
    assert isinstance(result.dimensions, list)
    assert all(isinstance(dimension, Dimension) for dimension in result.dimensions)

    assert result.query == clean_query(query)
    assert len(result.dimensions) > 0

