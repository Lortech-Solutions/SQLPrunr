# SQLPrunr

Tool for visualizing and optimizing queries.

# Usage

## Parse Snowflake database

```python
import csv
from sqlprunr.engine.parser import SnowflakeCSVTableParser

# Parsing database schema, exported data from snowflake comes with following format
# DATABASE_NAME,SCHEMA_NAME,TABLE_NAME,COLUMN_NAME,DATA_TYPE
with open(r"snowflake_database_schema.csv", "r") as f:
    data = f.read()
    reader = csv.DictReader(io.StringIO(data.strip()))

data = [SnowflakeCSVData(**row) for row in reader]
databases = parser.parse_table(data)
```

## Analyze queries

```python
import csv
from sqlprunr.engine.analyzer import analyze_query


# QUERY_TEXT,START_TIME,END_TIME
with open("queries.csv", "r") as f:
  query = f.read()

  reader = csv.DictReader(io.StringIO(query.strip()))
  query_data = [QueryData(**row) for row in reader]

# Analyzing queries is useless in most scenarios, it allows you to only analyze single query per call and the data alone does not provide any useful info
for query in query_data:
  data = analyze_query(query)
  print(data)  # { "query_ref": query, "execution_time": 0, "tables": [Table(name=..., columns=[...])], "columns": Column(name=..., data_type=...) }

# However using get_frequencies combined with queries data can provide you a lot of valuable data, like how many times such table/column were used in queries 
frequencies = get_frequencies(queries, tables=True, columns=True)
print(frequencies)  # Frequencies(tables={"table1": 1}, columns={"column1": 1}, queries={...})

# Using Frequency object combined with Database object parsed earlier we can easily discover which tables/columns were never used in any query

unused_tables = find_unused_tables(frequencies, database)  # Keep in mind database is single Database object, parser returns a list of Databases. You need to use proper database that's related to queries data.
print(unused_tables)  # [Table(name=..., columns=[Column(name=..., data_type=...), ...])]
```

## Return data as JSON/Dict

Most of the SQLPrunr objects are dataclasses, however in particular scenarios you will need to serialize this data.
The easiest way to do that is to use `dataclasses.asdict()` method.

```py
from dataclasses import asdict

frequencies = get_frequencies(queries, tables=True, columns=True)
print(asdict(frequencies))  # {'tables': {'table1': 1}, 'columns': {'column1': 1}, 'queries': {...}}
```

## Performance

Tools that work with a lot of data are meant to be fast, or at least their time complexity should not scale exponentialy

### General benchmark

General benchmark shows that snowflake parser can parse over 30k rows/s (with logging turned off) and return them as a complete Database object.
Function responsible for finding unused tables can handle over 4k operations/s [Possible O(n^2)!!!]
The worst in this benchmark is our fundamental function, however it's speed is still acceptable due to constant in best or linearithmic in worst, time complexity

```
------------------------------------------------------------------------------------------- benchmark: 3 tests -------------------------------------------------------------------------------------------
Name (time in us)                Min                    Max                Mean              StdDev              Median                IQR            Outliers  OPS (Kops/s)            Rounds  Iterations
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
test_snowflake_parse_table             26.5050 (1.0)         339.2960 (1.0)       31.8312 (1.0)        8.3035 (1.0)       29.2470 (1.0)       1.8890 (1.0)     1105;1188       31.4157 (1.0)        9861           1
test_find_unused_tables     153.2770 (5.78)     17,399.9130 (51.28)    175.1263 (5.50)     351.0254 (42.27)    165.6660 (5.66)      7.3580 (3.90)        3;126        5.7102 (0.18)       2422           1
test_analyze_query          922.6030 (34.81)     1,306.2310 (3.85)     970.5731 (30.49)     65.3854 (7.87)     949.8450 (32.48)    33.9240 (17.96)         6;6        1.0303 (0.03)         82           1
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Legend:
  Outliers: 1 Standard Deviation from Mean; 1.5 IQR (InterQuartile Range) from 1st Quartile and 3rd Quartile.
  OPS: Operations Per Second, computed as 1 / Mean
```

