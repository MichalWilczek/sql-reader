"""Logic for communication between the databases."""
import logging


class DatabaseSynchronizer:
    """Object synchronizing the workflow between two databases.

    Parameters
    ----------
    database_writer : InfluxClient
        Interface for saving the data to the database.
    database_reader : SQLClient
        Interface for reading the data from the database.
    """

    def __init__(self, database_writer, database_reader):
        self._logger = logging.getLogger(__name__)
        self._save_client = database_writer
        self._read_client = database_reader

    def move_data_to_db(self, table_name, tag_columns=None, delete_uploaded_data=True):
        """Move data from one database to another.

        The logic is the following.
        1. Read all available data from database 1 (reader) starting from
           the last recorded timestamp in database 2 (saver).
        2. Save the uploaded data to database 2 (saver).
        3. Delete the saved data to database 2 (saver) from database 1 (reader).
           This procedure is optional.

        Parameters
        ----------
        table_name : str
            Name of the table.
        tag_columns : list(str), optional
            List of columns to be tagged in the database, to which
            the data are saved. Default value is None.
        delete_uploaded_data : bool, optional
            Specifies whether the data uploaded from the database
            should be deleted. Default value is True.
        """
        try:
            query_start_timestamp = self._save_client.get_latest_measurement_timestamp(table_name)
            sql_data = self._read_client.query_data(
                table_name=table_name,
                start_timestamp=query_start_timestamp
            )

            self._save_client.write_points(
                dataframe=sql_data,
                measurement=table_name,
                tag_columns=tag_columns,
                batch_size=5000
            )

            if delete_uploaded_data:
                query_end_timestamp = self._save_client.get_latest_measurement_timestamp(table_name)
                self._read_client.delete_data(
                    table_name=table_name,
                    start_timestamp=query_start_timestamp,
                    end_timestamp=query_end_timestamp
                )

        except Exception as e:
            self._logger.exception(str(e))
