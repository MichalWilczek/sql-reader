"""Logic for communication with the Influx database."""
import logging
import pandas as pd
from influxdb import DataFrameClient


class InfluxClient(DataFrameClient):
    """Custom implementation of InfluxDBClient to add some additional features.
    Refer to InfluxDBClient documentation for further information.
    """

    def __init__(self, **kwargs):
        self._logger = logging.getLogger(__name__)
        super().__init__(**kwargs)

    @classmethod
    def init_from_dict(cls, kwargs):
        """Delete keyword arguments for InfluxDBClient if any of them
        has a None value.

        Returns
        -------
        InfluxDBWriter
            Initialised class object.
        """
        kwargs = kwargs["client_data"]
        new_kwargs = {key: value for key, value in kwargs.items() if value is not None}
        return cls(**new_kwargs)

    def get_latest_measurement_timestamp(self, table_name):
        """Get the latest timestamp of the saved data from
        the database table, given its name.

        Parameters
        ----------
        table_name : str
            Name of the table.

        Returns
        -------
        pandas.Timestamp
            Date format with nanoseconds.
        """
        query = f"SELECT * from {table_name} ORDER BY DESC LIMIT 1"
        try:
            results = self.query(query)[table_name]
            return pd.Timestamp(results.index.values[0])

        except KeyError:
            self._logger.info(f"There is currently no table: {table_name} in the database.")
            return None
        except Exception as e:
            self._logger.exception(str(e))
            raise
