# sql-reader
##### Package reading an SQL database to Python.

### Project Description

The project was developed to have an already configured tool for:
    1) reading data from an SQL database of any type, 
    2) setting automatic tests on GitHub.

### Installation Requirements

The program was developed with Python 3.7
```console
# clone the repo
$ git clone https://github.com/MichalWilczek/sql-reader.git

# change the working directory to sql-reader
$ cd sql-reader

# install the requirements
$ python -m pip install -r requirements.txt
```

### Development

The current version of the code is too small to have an external documentation. 
In case of further development, it is recommended to create the following directory: doc/architecture.

### Example

```python
from sql_reader import SQLReader

# Instantiate the database client.
# Here, it's an sqlite database, but can be any SQL-based database.
sql_client = SQLReader(engine="sqlite:///db_name.db")

# Query three columns from the table, one being an index column in pandas.DataFrame.
data = sql_client.query_data(
    table_name="driveline_power_data",
    column_names=["sensor_id", "rotational_speed"],
    index_column_name="timestamp",
    index_timestamp_unit="ns"
)

# Empty the queried table.
sql_client.empty_table(
    table_name="driveline_power_data"
)
```
