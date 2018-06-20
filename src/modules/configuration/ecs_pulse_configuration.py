"""
DELL EMC ECS API Data Collection Module.
"""
import logging
import os
import json

"""
Constants Section
"""
BASE_CONFIG = 'BASE'                                          # Base Configuration Section
ECS_CONNECTION_CONFIG = 'ECS_CONNECTION'                      # ECS Connection Configuration Section
DATABASE_CONNECTION_CONFIG = 'DATABASE_CONNECTION'            # Influx Database Connection Configuration Section


class InvalidConfigurationException(Exception):
    pass


class Configuration(object):
    def __init__(self, config):

        if config is None:
            raise InvalidConfigurationException("No file path to the ECS Data Collection Module configuration provided")

        if not os.path.exists(config):
            raise InvalidConfigurationException("The ECS Data Collection Module configuration "
                                                "file path does not exist: " + config)

        # Attempt to open configuration file
        try:
            with open(config, 'r') as f:
                parser = json.load(f)
        except Exception as e:
            raise InvalidConfigurationException("The following unexpected exception occurred in the "
                                                "ECS Data Collection Module attempting to parse "
                                                "the configuration file: " + e.message)

        # We parsed the configuration file now lets grab values
        self.protocol = parser[ECS_CONNECTION_CONFIG]['protocol']
        self.host = parser[ECS_CONNECTION_CONFIG]['host']
        self.port = parser[ECS_CONNECTION_CONFIG]['port']
        self.user = parser[ECS_CONNECTION_CONFIG]['user']
        self.password = parser[ECS_CONNECTION_CONFIG]['password']

        # Set logging level
        logging_level_raw = parser[BASE_CONFIG]['logging_level']
        self.logging_level = logging.getLevelName(logging_level_raw.upper())

        # Validate values
        if not self.protocol:
            raise InvalidConfigurationException("The ECS Management protocol is not "
                                                "configured in the module configuration")
        if not self.host:
            raise InvalidConfigurationException("The ECS Management Host is not configured in the module configuration")
        if not self.port:
            raise InvalidConfigurationException("The ECS Management port is not configured in the module configuration")
        if not self.user:
            raise InvalidConfigurationException("The ECS Management User is not configured in the module configuration")
        if not self.password:
            raise InvalidConfigurationException("The ECS Management Users password is not configured "
                                                "in the module configuration")
        if logging_level_raw not in ['debug', 'info', 'warning', 'error']:
            raise InvalidConfigurationException(
                "Logging level can be only one of ['debug', 'info', 'warning', 'error']")
