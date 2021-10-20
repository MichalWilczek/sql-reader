"""Main script for the application."""
import argparse
import json
import time

from sql_client import SQLClient
from influx_client import InfluxClient
from database_synchronizer import DatabaseSynchronizer


def parse_arguments():
    """Parse command-line arguments.

    Returns
    -------
    argparse.Namespace:
        Object containing parsed arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-cfg",
                        "--configuration_file_path",
                        required=True,
                        type=str,
                        dest="config_path",
                        help="Path to the config file.")
    return parser.parse_args()


def run():
    """Run the application."""

    # Initialize the configuration for the application.
    args = parse_arguments()
    cfg_path = args.config_path
    with open(cfg_path, "r") as input_file:
        cfg = json.load(input_file)

    # Instantiate the objects communicating with the databases.
    sql_client = SQLClient.init_from_dict(cfg["sqlite"])
    influx_client = InfluxClient.init_from_dict(cfg["influx"])
    influx_synchronizer = DatabaseSynchronizer(influx_client, sql_client)

    # Start an infinite loop moving the data from one database to another.
    tables = cfg["sqlite"]["tables"]
    while True:
        for table in tables.keys():
            influx_synchronizer.move_data_to_db(
                table_name=table,
                tag_columns=tables[table]["tags"]
            )
        time.sleep(cfg["synchronization"]["sleep_time_seconds"])
