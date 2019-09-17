"""
DELL EMC ECS API Data Collection Module.
"""
from influxdb import InfluxDBClient
import requests
from requests.auth import HTTPBasicAuth


class InfluxException(Exception):
    pass


class InfluxUtility(object):
    """
    Stores ECS Authentication Information
    """
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

    def check_db_exists(self, name):
        """
        Checks if a database exists and returns a boolean
        """
        try:
            influx_client = InfluxDBClient(self.config.database_host, self.config.database_port,
                                           self.config.database_user, self.config.database_password)

            db_list = influx_client.get_list_database()

            for db in db_list:
                if db['name'] == name:
                    self.logger.info('InfluxUtility::check_db_exists()::Database ' + name + ' found.')
                    return True

            self.logger.info('InfluxUtility::check_db_exists()::Database ' + name + ' not found.')
            return False

        except Exception as e:
            self.logger.error('InfluxUtility::check_db_exists()::The following '
                              'unhandled exception occured: ' + e.message)
            return False

    def write_point_data(self, name):
        """
        Central point to manage write points data
        """
        try:
            influx_client = InfluxDBClient(self.config.database_host, self.config.database_port,
                                           self.config.database_user, self.config.database_password)

            db_list = influx_client.get_list_database()

            for db in db_list:
                if db['name'] == name:
                    self.logger.info('InfluxUtility::check_db_exists()::Database ' + name + ' found.')
                    return True

            self.logger.info('InfluxUtility::check_db_exists()::Database ' + name + ' not found.')
            return False

        except Exception as e:
            self.logger.error('InfluxUtility::check_db_exists()::The following '
                              'unhandled exception occured: ' + e.message)
            return False
