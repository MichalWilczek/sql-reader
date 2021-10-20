"""Logic for communication with the SQL database."""
import logging
import pandas as pd
from sqlalchemy import create_engine


class SQLClient:
    """Client object for the SQL database.

    Parameters
    ----------
    engine : str
        Path for the SQL database, e.g.
        - "sqlite:///test_db.db" (for sqlite implementation).
    """

    def __init__(self, engine):
        self._logger = logging.getLogger(__name__)
        self._engine = create_engine(engine, echo=False)

    @classmethod
    def init_from_dict(cls, cfg):
        """Instantiate the class from the configuration dictionary.

        Parameters
        ----------
        cfg : dict
            Configuration dictionary.

        Returns
        -------
        SQLClient
            Instantiated class.
        """
        return cls(cfg["engine"])

    @staticmethod
    def _create_timestamp_sql_condition(start_timestamp=None, end_timestamp=None):
        query_message = ""
        query_options = []
        if start_timestamp is not None:
            query_options.append(f"timestamp >= {start_timestamp.value} ")
        if end_timestamp is not None:
            query_options.append(f"timestamp <= {end_timestamp.value} ")
        if len(query_options) > 0:
            query_message = "WHERE " + "AND ".join(query_options)
        return query_message

    def query_data(self, table_name, start_timestamp=None, end_timestamp=None):
        """Get data from the database.

        Parameters
        ----------
        table_name : str
            Name of the table.
        start_timestamp : pandas.Timestamp, optional
            Start timestamp from which the data should queried.
            Default value is None.
        end_timestamp : pandas.Timestamp, optional
            End timestamp from which the data should queried.
            Default value is None.

        Returns
        -------
        pandas.DataFrame
            Queried data from the database.
        """
        query_message = f"SELECT * FROM {table_name} " \
                        f"{self._create_timestamp_sql_condition(start_timestamp, end_timestamp)}"
        try:
            with self._engine.connect() as con:
                data = pd.read_sql(
                    sql=query_message,
                    con=con,
                    index_col="timestamp",
                    parse_dates={"timestamp": "ns"}
                )
                return data
        except Exception as e:
            self._logger.exception(str(e))
            raise

    def delete_data(self, table_name, start_timestamp=None, end_timestamp=None):
        """Remove the data from the database.

        Parameters
        ----------
        table_name : str
            Name of the table.
        start_timestamp : pandas.Timestamp, optional
            Start timestamp from which the data should deleted.
            Default value is None.
        end_timestamp : pandas.Timestamp, optional
            End timestamp from which the data should deleted.
            Default value is None.
        """
        query_message = f"DELETE FROM {table_name} " \
                        f"{self._create_timestamp_sql_condition(start_timestamp, end_timestamp)}"
        try:
            with self._engine.connect() as con:
                con.execute(query_message)
        except Exception as e:
            self._logger.exception(str(e))
            raise
