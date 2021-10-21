"""Logic for communication with the SQL database."""
import logging
import pandas as pd
from sqlalchemy import create_engine


class SQLReader:
    """Client object that reads data from an SQL database.

    Parameters
    ----------
    engine : str
        Path for the SQL database, e.g.
        - "sqlite:///test_db.db" (for sqlite implementation).
    """

    def __init__(self, engine):
        self._logger = logging.getLogger(__name__)
        self._engine = create_engine(engine, echo=False)

    @staticmethod
    def _create_message_for_columns(column_names=None, index_column_name=None):
        if index_column_name is not None and index_column_name not in column_names:
            column_names.append(index_column_name)

        if column_names is None:
            return "*"
        return ",".join(column_names)

    @staticmethod
    def _check_timestamp_unit(unit):
        allowed_units = ["D", "h", "m", "s", "ms", "us", "ns"]
        if unit not in allowed_units:
            raise ValueError(
                f"The parameter 'timestamp_unit' should be "
                f"in one of the allowed units: {allowed_units}."
                f"The input unit is: {unit}"
            )

    @staticmethod
    def _check_column_names_type(column_names):
        if not isinstance(column_names, list):
            raise TypeError(
                "The parameter 'column_names' should be a list. "
                f"Now, the type is: {type(column_names)}."
            )

    def query_data(self,
                   table_name,
                   column_names=None,
                   index_column_name=None,
                   index_timestamp_unit=None,
                   where_condition=""):
        """Get data from the database.

        Parameters
        ----------
        table_name : str
            Name of the table to read.
        column_names : list(str), optional
            List of column names to be read from the SQL table.
            By default, the entire table is read.
        index_column_name : str, optional
            The column name to be transformed to an index column.
            By default, no column is used for this operation.
            If the specified column name was not specified in column_names,
            it is added to the query.
        index_timestamp_unit : str, optional
            Time unit of the index column if it is chosen. The valid
            units are ‘D’, ‘h’, ‘m’, ‘s’, ‘ms’, ‘us’, and ‘ns’.
            For example, ‘s’ means seconds and ‘ms’ means milliseconds.
        where_condition : str, optional
            An additional SQL condition that can be put to the query.

        Returns
        -------
        pandas.DataFrame
            Queried data from the database.

        Raises
        ------
        ValueError
            If index_timestamp_unit has a wrong unit.
        TypeError
            If column_names is not a list.
        """
        if column_names is not None:
            self._check_column_names_type(column_names)
        query_message = f"SELECT {self._create_message_for_columns(column_names, index_column_name)} " \
                        f"FROM {table_name} " \
                        f"{where_condition}"

        if index_timestamp_unit is None or \
                (index_column_name is None and index_timestamp_unit is not None):
            parse_dates = None
        else:
            self._check_timestamp_unit(index_timestamp_unit)
            parse_dates = {f"{index_column_name}": index_timestamp_unit}

        try:
            with self._engine.connect() as con:
                data = pd.read_sql(
                    sql=query_message,
                    con=con,
                    index_col=index_column_name,
                    parse_dates=parse_dates
                )
                return data

        except Exception as e:
            self._logger.exception(str(e))
            raise

    def empty_table(self, table_name, where_condition=""):
        """Remove the data from the database.

        Parameters
        ----------
        table_name : str
            Name of the table to read.
        where_condition : str
            An additional SQL condition that can be put to the query.
        """
        query_message = f"DELETE FROM {table_name} {where_condition}"
        try:
            with self._engine.connect() as con:
                con.execute(query_message)

        except Exception as e:
            self._logger.exception(str(e))
            raise
