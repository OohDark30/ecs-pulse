"""
DELL EMC ECS API Data Collection Module.
"""
from modules.configuration.ecs_pulse_configuration import Configuration
from modules.logger import ecs_logger
from modules.ecs.ecs import ECSAuthentication
from modules.ecs.ecs import ECSManagementAPI
import os
import traceback
import sys
import signal
import time
import logging
import socket
import sys
import json


"""
Constants Section
"""
MODULE_NAME = "ECS_Data_Collection_Module"                  # Module Name
INTERVAL = 20                                               # In seconds
CONFIG_FILE = 'ecs_pulse_config.json'                       # Default Configuration File

"""
Globals Section
"""
_configuration = None
_ecsManagementNode = None
_ecsManagementUser = None
_ecsManagementUserPassword = None
_logger = None
_ecsAuthentication = None

"""
Class to listen for signal termination for controlled shutdown
"""


class ControlledShutdown:

    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.controlled_shutdown)
        signal.signal(signal.SIGTERM, self.controlled_shutdown)

    def controlled_shutdown(self,signum, frame):
        self.kill_now = True


def ecs_config(config):
    global _configuration
    global _logger
    global _ecsAuthentication

    """ 
    Use Configuration object to load and validate configuration data
    """
    _configuration = Configuration(config)

    _logger = ecs_logger.get_logger(__name__, _configuration.logging_level)
    _logger.info(MODULE_NAME + '::ecs_config()::We have configured logging level to: '
                 + logging.getLevelName(str(_configuration.logging_level)))
    _logger.info(MODULE_NAME + '::ecs_config()::Configuring ECS Data Collection Module complete.')


def ecs_read():
    """
    Validate that we have successfully configured and perform operations
    :param data:
    :return:
    """

    global _ecsAuthentication
    global _logger
    ecsmgmntapi = ECSManagementAPI(_ecsAuthentication, _logger)

    if not _ecsAuthentication:
        _logger.info(MODULE_NAME + '::ecs_read()::ECS Data Collection Module is not ready.  Please check logs.')
        return

    """
    Grab ECS Dashboard Local Zone Data
    """
    zonedata = ecsmgmntapi.localzone()

    if zonedata is None:
        _logger.info(MODULE_NAME + '::ecs_read()::Unable to retrieve ECS Dashboard Local Zone Information')
        return
    else:
        """
        We have zone data now lets show it
        """
        # val = collectd.Values(type='gauge', type_instance='totalDisks')
        # val.plugin = PLUGIN_NAME + "-DiskInfo"
        # val.values = [zonedata["numDisks"]]
        # val.dispatch()
        # val = collectd.Values(type='gauge', type_instance='badDisks')
        # val.plugin = PLUGIN_NAME + "-DiskInfo"
        # val.values = [zonedata["numBadDisks"]]
        # val.dispatch()
        # val = collectd.Values(type='gauge', type_instance='goodDisks')
        # val.plugin = PLUGIN_NAME + "-DiskInfo"
        # val.values = [zonedata["numGoodDisks"]]
        # val.dispatch()

        # try:
        #     sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # except socket.error, msg:
        #     _logger.info('ECS Python Plugin::ecs_read()::Unable to create a socket for the following reason: \n' + msg[1])
        #
        # try:
        #     sock.connect(('10.246.23.215', 9563))
        # except socket.error, msg:
        #     _logger.info('ECS Python Plugin::ecs_read()::Unable to open a socket connection '
        #                  'for the following reason: \n' + msg[1])
        #
        # sentbytes = sock.send(str(json.dumps(zonedata.json())).encode('utf-8'))
        # _logger.debug('ECS Python Plugin::ecs_read()::Socket call to Logstash server '
        #              'completed with ' + str(sentbytes) + ' sent.')

    """
    Grab ECS Capacity Data
    """
    # capacitydata = ecsmgmntapi.capacity()
    #
    # if capacitydata is None:
    #     _logger.info('Unable to retrieve ECS Capacity Information')
    #     return
    # else:
    #     val = collectd.Values(type='gauge', type_instance='totalFree_gb')
    #     val.plugin = PLUGIN_NAME + "-CapacityInfo"
    #     val.values = [capacitydata["totalFree_gb"]]
    #     val.dispatch()
    #     val = collectd.Values(type='gauge', type_instance='totalProvisioned_gb')
    #     val.plugin = PLUGIN_NAME + "-CapacityInfo"
    #     val.values = [capacitydata["totalProvisioned_gb"]]
    #     val.dispatch()


def ecs_init():
    """
    collectd initialization callback method
    """
    global _ecsAuthentication
    global _configuration
    global _logger

    try:
        while not _configuration:
            time.sleep(1)

        auth = ECSAuthentication(_configuration.protocol, _configuration.host, _configuration.user,
                                 _configuration.password, _configuration.port, _logger)
        auth.connect()

        if auth.token is None:
            _logger.info(MODULE_NAME + '::ecs_init()::Unable to authenticate to ECS as configured.  '
                         'Please validate and try again.')
        else:
            _logger.info(MODULE_NAME + '::ecs_init()::Successfully authenticated to ECS')
            _ecsAuthentication = auth

    except Exception as e:
        _logger.error(MODULE_NAME + '::ecs_init()::Cannot initialize plugin. Cause: '
                      + str(e) + "\n" + traceback.format_exc())


"""
Main 
"""
if __name__ == "__main__":

    # Create object to support controlled shutdown
    controlledShutdown = ControlledShutdown()

    # Dump out application path
    currentApplicationDirectory = os.getcwd()
    configFilePath = os.path.abspath(os.path.join(currentApplicationDirectory, "config", CONFIG_FILE))
    print(MODULE_NAME + "__main__::Current directory is : " + currentApplicationDirectory)
    print(MODULE_NAME + "__main__::Configuration file path is: " + configFilePath)

    # Load default configuration file
    ecs_config(configFilePath)

    # Initialize connection to ECS
    ecs_init()

    # Setup up while loop to read data at set intervals
    while True:
        time.sleep(INTERVAL)
        ecs_read()
        if controlledShutdown.kill_now:
            break



