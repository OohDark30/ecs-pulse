"""
DELL EMC ECS API Data Collection Module.
"""

from configuration.ecs_pulse_configuration import ECSPulseConfiguration
from logger import ecs_logger
from ecs.ecs import ECSAuthentication
from ecs.ecs import ECSManagementAPI
from ecs.ecs import ECSUtility
from influx.influx import InfluxUtility
from influxdb import InfluxDBClient
import errno
import datetime
import os
import traceback
import signal
import time
import logging
import threading
import xml.etree.ElementTree as ET

# Constants
MODULE_NAME = "ECS_Data_Collection_Module"                  # Module Name
INTERVAL = 30                                               # In seconds
CONFIG_FILE = 'ecs_pulse_config.json'                       # Default Configuration File
VDC_LOOKUP_FILE = 'ecs_vdc_lookup.json'                     # VDC ID Lookup File

# Globals
_configuration = None
_ecsManagementNode = None
_ecsManagementUser = None
_ecsManagementUserPassword = None
_logger = None
_ecsAuthentication = list()
_influxClient = None
_ecsVDCLookup = None
_ecsManagmentAPI = list()

"""
Class to listen for signal termination for controlled shutdown
"""


class ECSDataCollectionShutdown:

    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.controlled_shutdown)
        signal.signal(signal.SIGTERM, self.controlled_shutdown)

    def controlled_shutdown(self, signum, frame):
        self.kill_now = True


class ECSDataCollection(threading.Thread):
    def __init__(self, method, influxclient, logger, ecsmanagmentapi, pollinginterval, tempdir):
        threading.Thread.__init__(self)
        self.method = method
        self.influxclient = influxclient
        self.logger = logger
        self.ecsmanagmentapi = ecsmanagmentapi
        self.pollinginterval = pollinginterval
        self.tempdir = tempdir

        logger.info(MODULE_NAME + '::ECSDataCollection()::init method of class called')

    def run(self):
        try:
            self.logger.info(MODULE_NAME + '::ECSDataCollection()::Starting thread with method: ' + self.method)

            if self.method == 'ecs_collect_capacity_data()':
                ecs_collect_capacity_data(self.influxclient, self.logger, self.ecsmanagmentapi, self.pollinginterval)
            else:
                if self.method == 'ecs_collect_local_zone_data()':
                    ecs_collect_local_zone_data(self.influxclient, self.logger, self.ecsmanagmentapi, self.pollinginterval)
                else:
                    if self.method == 'ecs_collect_local_zone_node_data()':
                        ecs_collect_local_zone_node_data(self.influxclient, self.logger, self.ecsmanagmentapi, self.pollinginterval)
                    else:
                        if self.method == 'ecs_collect_local_zone_disk_data()':
                            ecs_collect_local_zone_disk_data(self.influxclient, self.logger, self.ecsmanagmentapi, self.pollinginterval)
                        else:
                            if self.method == 'ecs_collect_local_zone_replication_data()':
                                ecs_collect_local_zone_replication_data(self.influxclient, self.logger, self.ecsmanagmentapi, self.pollinginterval)
                            else:
                                if self.method == 'ecs_collect_local_zone_replication_failure_data()':
                                    ecs_collect_local_zone_replication_failure_data(self.influxclient, self.logger, self.ecsmanagmentapi, self.pollinginterval)
                                else:
                                    if self.method == 'ecs_collect_local_zone_bootstrap_data()':
                                        ecs_collect_local_zone_bootstrap_data(self.influxclient, self.logger, self.ecsmanagmentapi, self.pollinginterval)
                                    else:
                                        if self.method == 'ecs_collect_namespace_billing_data()':
                                            ecs_collect_namespace_billing_data(self.influxclient, self.logger, self.ecsmanagmentapi, self.pollinginterval, self.tempdir)
                                        else:
                                            self.logger.info(MODULE_NAME + '::ECSDataCollection()::Requested method ' +
                                                             self.method + ' is not supported.')
        except Exception as e:
            _logger.error(MODULE_NAME + 'ECSDataCollection::run()::The following unexpected '
                                        'exception occured: ' + str(e) + "\n" + traceback.format_exc())


def ecs_check_for_integer(var_to_check):
    global _logger

    try:
        # Try and convert variable to integer
        int(var_to_check)
        return True
    except ValueError:
        return False


def ecs_delete_file(file_to_delete):
    global _logger

    try:
        # Load and validate module configuration
        os.remove(file_to_delete)

        _logger.debug(MODULE_NAME + '::ecs_delete_file()::Successfully deleted file ' + file_to_delete + '.')
    except OSError as e:
        if e.errno != errno.ENOENT:
            _logger.error(MODULE_NAME + '::ecs_delete_file()::The following unexpected '
                                        'exception occured: ' + str(e) + "\n" + traceback.format_exc())


def ecs_config(config, vdc_config, temp_dir):
    global _configuration
    global _logger
    global _ecsAuthentication
    global _ecsVDCLookup
    try:
        # Load and validate module configuration
        _configuration = ECSPulseConfiguration(config, temp_dir)

        # Load ECS VDC Lookup
        _ecsVDCLookup = ECSUtility(_ecsAuthentication, _logger, vdc_config)

        # Grab loggers and log status
        _logger = ecs_logger.get_logger(__name__, _configuration.logging_level)
        _logger.info(MODULE_NAME + '::ecs_config()::We have configured logging level to: '
                     + logging.getLevelName(str(_configuration.logging_level)))
        _logger.info(MODULE_NAME + '::ecs_config()::Configuring ECS Data Collection Module complete.')
    except Exception as e:
        _logger.error(MODULE_NAME + '::ecs_config()::The following unexpected '
                                    'exception occured: ' + str(e) + "\n" + traceback.format_exc())


def ecs_collect_capacity_data(influxclient, logger, ecsmanagmentapi, pollinginterval):

    try:
        # Start polling loop
        while True:
            # Perform API call against each configured ECS
            for ecsconnection in ecsmanagmentapi:
                # Retrieve capacity data via API
                capacity_data = ecsconnection.get_capacity_data()

                if capacity_data is None:
                    logger.info(MODULE_NAME + '::ecs_collect_capacity_data()::Unable to retrieve ECS Dashboard Capacity Information')
                    return
                else:
                    """
                    We have the raw JSON data now lets prep it for Influx
                    """

                    # Declare locals
                    current_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
                    current_epoch_time = time.time()
                    db_array = []
                    ecsdata = {}
                    ecsdata_metrics = {}
                    ecsdata_summary = {}
                    fields = {}
                    tags = {}
                    target_name = "Capacity"

                    # Grab VDC Name
                    tags['vdc'] = _ecsVDCLookup.vdc_json[ecsconnection.authentication.host]

                    # Process remaining data in JSON
                    for field in capacity_data:
                        # Process individual data field
                        if type(capacity_data[field]) is int:
                            try:
                                logger.debug(MODULE_NAME + '::ecs_collect_capacity_data()::field from capacity_data being processed is: ' + field)
                                ecsdata[field] = float(capacity_data[field])
                            except Exception:
                                try:
                                    # We're here because trying to convert to a float failed.  Store whatever value is there
                                    ecsdata[field] = capacity_data[field].encode("utf-8")
                                except Exception:
                                    pass
                        # Process list fields
                        elif type(capacity_data[field]) is list:
                            logger.debug(MODULE_NAME + '::ecs_collect_data()::field from capacity_data being processed is: ' + field)
                            ecsconnection.get_ecs_detail_data(field=field, metric_list=capacity_data[field], metric_values=ecsdata_metrics)
                        else:
                            # Process dictionary fields
                            logger.debug(MODULE_NAME + '::ecs_collect_data()::field from capacity_data being processed is: ' + field)
                            ecsconnection.get_ecs_summary_data(field=field, summary_dict=capacity_data[field],
                                                             current_epoch=current_epoch_time, summary_values=ecsdata_summary)

                    # Create Influx DB Info Dictionary for our string fields and add it to the db list
                    db_json = {
                        "measurement": target_name,
                        "tags": tags,
                        "fields": ecsdata,
                        "time": current_time
                    }
                    db_array.append(db_json.copy())

                    #  Create Influx DB Info Dictionary for our list fields and add it to the db list
                    for times in ecsdata_metrics:
                        influxdb_time = datetime.datetime.utcfromtimestamp(int(times))
                        influxdb_time = influxdb_time.strftime("%Y-%m-%dT%H:%M:%S")

                        db_json = {
                            "measurement": target_name+"Metrics",
                            "tags": tags,
                            "fields": ecsdata_metrics[times],
                            "time": influxdb_time
                        }
                        db_array.append(db_json.copy())

                    #  Create Influx DB Info Dictionary for our dictionary fields and add it to the db list
                    for times in ecsdata_summary:
                        influxdb_time = datetime.datetime.utcfromtimestamp(int(times))
                        influxdb_time = influxdb_time.strftime("%Y-%m-%dT%H:%M:%S")

                        db_json = {
                            "measurement": target_name+"Summary",
                            "tags": tags,
                            "fields": ecsdata_summary[times],
                            "time": influxdb_time
                        }
                        db_array.append(db_json.copy())

                    # Write data to Influx
                    influxclient.write_points(db_array)

                    # Dump array for debug
                    logger.debug(MODULE_NAME + '::ecs_collect_capacity_data()::'
                                               'Capacity db_array is: \r\n\r\n'.join(str(db_array)))

            if controlledShutdown.kill_now:
                print(MODULE_NAME + "ecs_collect_capacity_data()::Shutdown detected.  Terminating polling.")
                break

            # Wait for specific polling interval
            time.sleep(float(pollinginterval))
    except Exception as e:
        _logger.error(MODULE_NAME + '::ecs_collect_capacity_data()::The following unexpected '
                                    'exception occured: ' + str(e) + "\n" + traceback.format_exc())


def ecs_collect_local_zone_data(influxclient, logger, ecsmanagmentapi, pollinginterval):

    try:
        # Start polling loop
        while True:

            # Perform API call against each configured ECS
            for ecsconnection in ecsmanagmentapi:

                # Retrieve local zone data via API
                local_zone_data = ecsconnection.get_local_zone_data()

                if local_zone_data is None:
                    logger.error(MODULE_NAME + '::ecs_collect_local_zone_data()::'
                                               'Unable to retrieve ECS Dashboard Local Zone Information')
                    return
                else:
                    """
                    We have the raw JSON data now lets prep it for Influx
                    """

                    # Declare locals
                    current_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
                    current_epoch_time = time.time()
                    db_array = []
                    ecsdata = {}
                    ecsdata_metrics = {}
                    ecsdata_summary = {}
                    fields = {}
                    tags = {}
                    target_name = "dashboard_local_zone"

                    # Remove data points from raw json we are not interested in
                    local_zone_data.pop('_links', None)
                    local_zone_data.pop('transactionErrors', None)
                    local_zone_data.pop('transactionErrorsSummary', None)
                    local_zone_data.pop('transactionErrorsCurrent', None)

                    # Grab VDC Name
                    tags['vdc'] = _ecsVDCLookup.vdc_json[ecsconnection.authentication.host]

                    # Process remaining data in JSON
                    for field in local_zone_data:
                        # Process individual data field
                        if type(local_zone_data[field]) is str:
                            try:
                                logger.debug(MODULE_NAME + '::ecs_collect_local_zone_data()::'
                                                           'field from local_zone_data being processed is: ' + field)
                                ecsdata[field] = float(local_zone_data[field])
                            except Exception:
                                try:
                                    # We're here because trying to convert to a float failed.
                                    # Convert unicode value to string and store whatever value is there
                                    ecsdata[field] = local_zone_data[field].encode("utf-8")
                                except Exception:
                                    pass
                        # Process list fields
                        elif type(local_zone_data[field]) is list:
                            logger.debug(MODULE_NAME + '::ecs_collect_local_zone_data()::'
                                                       'field from local_zone_data being processed is: ' + field)
                            ecsconnection.get_ecs_detail_data(field=field, metric_list=local_zone_data[field], metric_values=ecsdata_metrics)
                        else:
                            # Process dictionary fields
                            logger.debug(MODULE_NAME + '::ecs_collect_local_zone_data()::'
                                                       'field from local_zone_data being processed is: ' + field)
                            ecsconnection.get_ecs_summary_data(field=field, summary_dict=local_zone_data[field],
                                                             current_epoch=current_epoch_time, summary_values=ecsdata_summary)

                    # Create Influx DB Info Dictionary for our string fields and add it to the db list
                    db_json = {
                        "measurement": target_name,
                        "tags": tags,
                        "fields": ecsdata,
                        "time": current_time
                    }
                    db_array.append(db_json.copy())

                    #  Create Influx DB Info Dictionary for our list fields and add it to the db list
                    for times in ecsdata_metrics:
                        influxdb_time = datetime.datetime.utcfromtimestamp(int(times))
                        influxdb_time = influxdb_time.strftime("%Y-%m-%dT%H:%M:%S")

                        db_json = {
                            "measurement": target_name+"_metrics",
                            "tags": tags,
                            "fields": ecsdata_metrics[times],
                            "time": current_time
                        }
                        db_array.append(db_json.copy())

                    #  Create Influx DB Info Dictionary for our dictionary fields and add it to the db list
                    for times in ecsdata_summary:
                        influxdb_time = datetime.datetime.utcfromtimestamp(int(times))
                        influxdb_time = influxdb_time.strftime("%Y-%m-%dT%H:%M:%S")

                        db_json = {
                            "measurement": target_name+"_summary",
                            "tags": tags,
                            "fields": ecsdata_summary[times],
                            "time": current_time
                        }
                        db_array.append(db_json.copy())

                    # Write data to Influx
                    influxclient.write_points(db_array)

                    # Dump array for debug
                    logger.debug(MODULE_NAME + '::ecs_collect_local_zone_data()::'
                                               'Local Zone db_array is: \r\n\r\n'.join(str(db_array)))

            if controlledShutdown.kill_now:
                print(MODULE_NAME + "ecs_collect_local_zone_data()::"
                                    "Shutdown detected.  Terminating polling.")
                break

            # Wait for specific polling interval
            time.sleep(float(pollinginterval))
    except Exception as e:
        _logger.error(MODULE_NAME + '::ecs_collect_local_zone_data()::The following unexpected '
                                    'exception occured: ' + str(e) + "\n" + traceback.format_exc())


def ecs_collect_local_zone_node_data(influxclient, logger, ecsmanagmentapi, pollinginterval):

    try:
        # Start polling loop
        while True:

            # Perform API call against each configured ECS
            for ecsconnection in ecsmanagmentapi:

                # Retrieve local zone node data via API
                local_zone_node_data = ecsconnection.get_local_zone_node_data()

                if local_zone_node_data is None:
                    logger.error(MODULE_NAME + '::ecs_collect_local_zone_node_data()::'
                                               'Unable to retrieve ECS Dashboard Local Zone Node Information')
                    return
                else:
                    """
                    We have the raw JSON data now lets prep it for Influx
                    """

                    # Declare locals
                    current_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
                    current_epoch_time = time.time()
                    db_array = []
                    ecsdata = {}
                    ecsdata_metrics = {}
                    ecsdata_summary = {}
                    fields = {}
                    tags = {}
                    target_name = "LocalZoneNodes"

                    tags['vdc'] = _ecsVDCLookup.vdc_json[ecsconnection.authentication.host]

                    # Grab just node information
                    zone_node_data = local_zone_node_data['_embedded']['_instances']

                    # Using 'local_zone_node_data' so we can re-use code without changing references
                    for local_zone_node_data in zone_node_data:

                        # Not handling a few metrics for now
                        local_zone_node_data.pop('_links', None)
                        local_zone_node_data.pop('transactionErrors', None)
                        local_zone_node_data.pop('transactionErrorsSummary', None)
                        local_zone_node_data.pop('transactionErrorsCurrent', None)

                        node_display_name = local_zone_node_data['displayName']
                        ecsdata[node_display_name] = {}
                        ecsdata_metrics[node_display_name] = {}
                        ecsdata_summary[node_display_name] = {}

                        for field in local_zone_node_data:
                            if type(local_zone_node_data[field]) is str:
                                try:
                                    logger.debug(MODULE_NAME + '::ecs_collect_local_zone_node_data()::field from '
                                                               'local_zone_node_data being processed is: ' + field)
                                    ecsdata[node_display_name][field] = float(local_zone_node_data[field])
                                except Exception as ex1:
                                    try:
                                        # We're here because trying to convert to a float failed.
                                        ecsdata[node_display_name][field] = local_zone_node_data[field].encode("utf-8")
                                    except Exception as ex2:
                                        pass

                            elif type(local_zone_node_data[field]) is list:
                                logger.debug(MODULE_NAME + '::ecs_collect_local_zone_node_data()::field from '
                                                           'local_zone_node_data being processed is: ' + field)

                                ecsconnection.get_ecs_detail_data(field=field, metric_list=local_zone_node_data[field],
                                                                metric_values=ecsdata_metrics[node_display_name])

                            else:
                                logger.debug(MODULE_NAME + '::ecs_collect_local_zone_node_data()::field from '
                                                           'local_zone_node_data being processed is: ' + field)

                                ecsconnection.get_ecs_summary_data(field=field, summary_dict=local_zone_node_data[field],
                                                                 current_epoch=current_epoch_time, summary_values=ecsdata_summary[node_display_name])

                    for node_display_name in ecsdata:
                        db_array = []
                        tags['NodeID'] = node_display_name
                        db_json = {
                            "measurement": target_name,
                            "tags": tags,
                            "fields": ecsdata[node_display_name],
                            "time": current_time
                        }
                        db_array.append(db_json.copy())
                        influxclient.write_points(db_array)
                        logger.debug(MODULE_NAME + '::ecs_collect_local_zone_node_data()::'
                                                   'Local Zone Node data db_array is: \r\n\r\n'.join(str(db_array)))

                    for node_display_name in ecsdata_metrics:
                        db_array = []
                        tags['NodeID'] = node_display_name

                        for times in ecsdata_metrics[node_display_name]:

                            influxdb_time = datetime.datetime.utcfromtimestamp(int(times))
                            influxdb_time = influxdb_time.strftime("%Y-%m-%dT%H:%M:%S")

                            db_json = {
                                "measurement": target_name+"Metrics",
                                "tags": tags,
                                "fields": ecsdata_metrics[node_display_name][times],
                                "time": influxdb_time
                            }
                            db_array.append(db_json.copy())

                        influxclient.write_points(db_array)
                    logger.debug(MODULE_NAME + '::ecs_collect_local_zone_node_data()::'
                                               'Local Zone Node metrics db_array is: \r\n\r\n'.join(str(db_array)))

                    for node_display_name in ecsdata_summary:
                        db_array = []
                        tags['NodeID'] = node_display_name

                        for times in ecsdata_summary[node_display_name]:
                            influxdb_time = datetime.datetime.utcfromtimestamp(int(times))
                            influxdb_time = influxdb_time.strftime("%Y-%m-%dT%H:%M:%S")

                            db_json = {
                                "measurement": target_name+"Summary",
                                "tags": tags,
                                "fields": ecsdata_summary[node_display_name][times],
                                "time": influxdb_time
                            }
                            db_array.append(db_json.copy())

                        influxclient.write_points(db_array)
                        logger.debug(MODULE_NAME + '::ecs_collect_local_zone_node_data()::'
                                                   'Local Zone Node summary db_array is: \r\n\r\n'.join(str(db_array)))

            if controlledShutdown.kill_now:
                print(MODULE_NAME + "ecs_collect_local_zone_node_data()::Shutdown detected.  Terminating polling.")
                break

            # Wait for specific polling interval
            time.sleep(float(pollinginterval))
    except Exception as e:
        _logger.error(MODULE_NAME + '::ecs_collect_local_zone_node_data()::The following unexpected '
                                    'exception occured: ' + str(e) + "\n" + traceback.format_exc())


def ecs_collect_local_zone_disk_data(influxclient, logger, ecsmanagmentapi, pollinginterval):

    while True:

        # Perform API call against each configured ECS
        for ecsconnection in ecsmanagmentapi:

            # Retrieve local zone disk data via API
            local_zone_disk_data = ecsconnection.get_local_zone_disk_data()

            if local_zone_disk_data is None:
                logger.error(MODULE_NAME + '::ecs_collect_local_zone_disk_data()::'
                                            'Unable to retrieve ECS Dashboard Local Zone Disk Information')
                return
            else:
                """
                We have the raw JSON data now lets prep it for Influx
                """

                # Declare locals
                current_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
                current_epoch_time = time.time()
                db_array = []
                ecsdata = {}
                ecsdata_metrics = {}
                ecsdata_summary = {}
                fields = {}
                tags = {}
                target_name = "LocalZoneDisks"

                tags['vdc'] = _ecsVDCLookup.vdc_json[ecsconnection.authentication.host]

                # Grab just node information
                zone_disk_data = local_zone_disk_data['_embedded']['_instances']

                # Using 'local_zone_disk_data' so we can re-use code without changing references
                for local_zone_disk_data in zone_disk_data:

                    # Not handling a few metrics for now
                    local_zone_disk_data.pop('_links', None)

                    disk_display_name = local_zone_disk_data['displayName']
                    ecsdata[disk_display_name] = {}
                    ecsdata_metrics[disk_display_name] = {}
                    ecsdata_summary[disk_display_name] = {}

                    for field in local_zone_disk_data:
                        if type(local_zone_disk_data[field]) is str:
                            try:
                                logger.debug(MODULE_NAME + '::ecs_collect_local_zone_disk_data()::field from '
                                                            'local_zone_disk_data being processed is: ' + field)
                                ecsdata[disk_display_name][field] = float(local_zone_disk_data[field])
                            except Exception as ex1:
                                try:
                                    # We're here because trying to convert to a float failed.
                                    ecsdata[disk_display_name][field] = local_zone_disk_data[field].encode("utf-8")
                                except Exception as ex2:
                                    pass

                        elif type(local_zone_disk_data[field]) is list:
                            logger.debug(MODULE_NAME + '::ecs_collect_local_zone_disk_data()::field from '
                                                        'local_zone_disk_data being processed is: ' + field)
                            ecsconnection.get_ecs_detail_data(field=field, metric_list=local_zone_disk_data[field],
                                                            metric_values=ecsdata_metrics[disk_display_name])

                        else:
                            logger.debug(MODULE_NAME + '::ecs_collect_local_zone_disk_data()::field from '
                                                        'local_zone_disk_data being processed is: ' + field)
                            ecsconnection.get_ecs_summary_data(field=field, summary_dict=local_zone_disk_data[field],
                                                             current_epoch=current_epoch_time, summary_values=ecsdata_summary[disk_display_name])

                for disk_display_name in ecsdata:
                    db_array = []
                    tags['DiskID'] = disk_display_name
                    db_json = {
                        "measurement": target_name,
                        "tags": tags,
                        "fields": ecsdata[disk_display_name],
                        "time": current_time
                    }
                    db_array.append(db_json.copy())
                    influxclient.write_points(db_array)
                    logger.debug(MODULE_NAME + '::ecs_collect_local_zone_disk_data()::Local Zone Failed Disk data db_array is: \r\n\r\n'.join(str(db_array)))

                for disk_display_name in ecsdata_metrics:
                    db_array = []
                    tags['DiskID'] = disk_display_name

                    for times in ecsdata_metrics[disk_display_name]:

                        influxdb_time = datetime.datetime.utcfromtimestamp(int(times))
                        influxdb_time = influxdb_time.strftime("%Y-%m-%dT%H:%M:%S")

                        db_json = {
                            "measurement": target_name+"Metrics",
                            "tags": tags,
                            "fields": ecsdata_metrics[disk_display_name][times],
                            "time": influxdb_time
                        }
                        db_array.append(db_json.copy())

                    influxclient.write_points(db_array)
                    logger.debug(MODULE_NAME + '::ecs_collect_local_zone_disk_data()::Local Zone Failed Disk metrics db_array is: \r\n\r\n'.join(str(db_array)))

                for disk_display_name in ecsdata_summary:
                    db_array = []
                    tags['DiskID'] = disk_display_name

                    for times in ecsdata_summary[disk_display_name]:
                        influxdb_time = datetime.datetime.utcfromtimestamp(int(times))
                        influxdb_time = influxdb_time.strftime("%Y-%m-%dT%H:%M:%S")

                        db_json = {
                            "measurement": target_name+"Summary",
                            "tags": tags,
                            "fields": ecsdata_summary[disk_display_name][times],
                            "time": influxdb_time
                        }
                        db_array.append(db_json.copy())

                    influxclient.write_points(db_array)
                    logger.debug(MODULE_NAME + '::ecs_collect_local_zone_disk_data()::'
                                               'Local Zone Failed Disk summary db_array is: \r\n\r\n'.join(str(db_array)))

        if controlledShutdown.kill_now:
            print(MODULE_NAME + "ecs_collect_local_zone_disk_data()::Shutdown detected.  Terminating polling.")
            break

        # Wait for specific polling interval
        time.sleep(float(pollinginterval))


def ecs_collect_local_zone_replication_data(influxclient, logger, ecsmanagmentapi, pollinginterval):

    while True:

        # Perform API call against each configured ECS
        for ecsconnection in ecsmanagmentapi:

            # Retrieve local zone replication data via API
            local_zone_replication_data = ecsconnection.get_local_zone_replication_data()

            if local_zone_replication_data is None:
                logger.error(MODULE_NAME + '::ecs_collect_local_zone_replication_data()::Unable to retrieve ECS Dashboard Local Replication Node Information')
                return
            else:
                """
                We have the raw JSON data now lets prep it for Influx
                """

                # Declare locals
                current_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
                current_epoch_time = time.time()
                db_array = []
                ecsdata = {}
                ecsdata_metrics = {}
                ecsdata_summary = {}
                fields = {}
                tags = {}
                target_name = "LocalZoneReplication"

                tags['vdc'] = _ecsVDCLookup.vdc_json[ecsconnection.authentication.host]

                # Grab just node information
                replication_data = local_zone_replication_data['_embedded']['_instances']

                # Using 'local_zone_replication_data' so we can re-use code without changing references
                for local_zone_replication_data in replication_data:

                    # Not handling a few metrics for now
                    local_zone_replication_data.pop('_links', None)

                    node_name = local_zone_replication_data['name']
                    ecsdata[node_name] = {}
                    ecsdata_metrics[node_name] = {}
                    ecsdata_summary[node_name] = {}

                    for field in local_zone_replication_data:
                        if type(local_zone_replication_data[field]) is str:
                            try:
                                logger.debug(MODULE_NAME + '::ecs_collect_local_zone_replication_data()::field from '
                                                            'local_zone_replication_data being processed is: ' + field)
                                ecsdata[node_name][field] = float(local_zone_replication_data[field])
                            except Exception as ex1:
                                try:
                                    # We're here because trying to convert to a float failed.
                                    ecsdata[node_name][field] = local_zone_replication_data[field].encode("utf-8")
                                except Exception as ex2:
                                    pass

                        elif type(local_zone_replication_data[field]) is list:
                            logger.debug(MODULE_NAME + '::ecs_collect_local_zone_replication_data()::field from '
                                                        'local_zone_replication_data being processed is: ' + field)
                            ecsconnection.get_ecs_detail_data(field=field, metric_list=local_zone_replication_data[field],
                                                            metric_values=ecsdata_metrics[node_name])

                        else:
                            logger.debug(MODULE_NAME + '::ecs_collect_data()::field from '
                                                        'local_zone_replication_data being processed is: ' + field)
                            ecsconnection.get_ecs_summary_data(field=field, summary_dict=local_zone_replication_data[field],
                                                             current_epoch=current_epoch_time, summary_values=ecsdata_summary[node_name])

                for node_name in ecsdata:
                    db_array = []
                    tags['ReplicationGroupID'] = node_name
                    db_json = {
                        "measurement": target_name,
                        "tags": tags,
                        "fields": ecsdata[node_name],
                        "time": current_time
                    }
                    db_array.append(db_json.copy())
                    influxclient.write_points(db_array)

                    # Dump array for debug
                    logger.debug(MODULE_NAME + '::ecs_collect_local_zone_replication_data()::Local Zone Replication field db_array is: \r\n\r\n'.join(str(db_array)))

                for node_name in ecsdata_metrics:
                    db_array = []
                    tags['ReplicationGroupID'] = node_name

                    for times in ecsdata_metrics[node_name]:

                        influxdb_time = datetime.datetime.utcfromtimestamp(int(times))
                        influxdb_time = influxdb_time.strftime("%Y-%m-%dT%H:%M:%S")

                        db_json = {
                            "measurement": target_name+"Metrics",
                            "tags": tags,
                            "fields": ecsdata_metrics[node_name][times],
                            "time": influxdb_time
                        }
                        db_array.append(db_json.copy())
                    influxclient.write_points(db_array)

                    # Dump array for debug
                    logger.debug(MODULE_NAME + '::ecs_collect_local_zone_replication_data()::Local Zone Replication metrics db_array is: \r\n\r\n'.join(str(db_array)))

                for node_name in ecsdata_summary:
                    db_array = []
                    tags['ReplicationGroupID'] = node_name

                    for times in ecsdata_summary[node_name]:
                        influxdb_time = datetime.datetime.utcfromtimestamp(int(times))
                        influxdb_time = influxdb_time.strftime("%Y-%m-%dT%H:%M:%S")

                        db_json = {
                            "measurement": target_name+"Summary",
                            "tags": tags,
                            "fields": ecsdata_summary[node_name][times],
                            "time": influxdb_time
                        }
                        db_array.append(db_json.copy())

                    influxclient.write_points(db_array)

                    # Dump array for debug
                    logger.debug(MODULE_NAME + '::ecs_collect_local_zone_replication_data()::'
                                                'Local Zone Replication summary db_array is: \r\n\r\n'.join(str(db_array)))

        if controlledShutdown.kill_now:
            print(MODULE_NAME + "ecs_collect_local_zone_replication_data()::Shutdown detected.  Terminating polling.")
            break

        # Wait for specific polling interval
        time.sleep(float(pollinginterval))


def ecs_collect_local_zone_replication_failure_data(influxclient, logger, ecsmanagmentapi, pollinginterval):

    try:
        while True:

            # Perform API call against each configured ECS
            for ecsconnection in ecsmanagmentapi:

                # Retrieve local zone failed replication data via API
                local_zone_failed_failed_replication_link_data = ecsconnection.get_local_zone_replication_failure_data()

                if local_zone_failed_failed_replication_link_data is None:
                    logger.error(MODULE_NAME + '::ecs_collect_local_zone_replication_failure_data()::'
                                               'Unable to retrieve ECS Dashboard Local Replication Group Link Failure Information')
                    return
                else:
                    """
                    We have the raw JSON data now lets prep it for Influx
                    """

                    # Declare locals
                    current_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
                    current_epoch_time = time.time()
                    db_array = []
                    ecsdata = {}
                    ecsdata_metrics = {}
                    ecsdata_summary = {}
                    fields = {}
                    tags = {}
                    target_name = "LocalZoneReplicationFailure"

                    tags['vdc'] = _ecsVDCLookup.vdc_json[ecsconnection.authentication.host]

                    # Grab just node information
                    failed_replication_link_data = local_zone_failed_failed_replication_link_data['_embedded']['_instances']

                    # Using 'local_zone_failed_failed_replication_link_data'
                    # so we can re-use code without changing references
                    for local_zone_failed_failed_replication_link_data in failed_replication_link_data:

                        # Not handling a few metrics for now
                        local_zone_failed_failed_replication_link_data.pop('_links', None)

                        failed_rg_name = local_zone_failed_failed_replication_link_data['rgName']
                        ecsdata[failed_rg_name] = {}
                        ecsdata_metrics[failed_rg_name] = {}
                        ecsdata_summary[failed_rg_name] = {}

                        for field in local_zone_failed_failed_replication_link_data:
                            if type(local_zone_failed_failed_replication_link_data[field]) is str:
                                try:
                                    logger.debug(MODULE_NAME + '::ecs_collect_local_zone_replication_failure_data()::field from '
                                                                'local_zone_failed_failed_replication_link_data being processed is: ' + field)
                                    ecsdata[failed_rg_name][field] = float(local_zone_failed_failed_replication_link_data[field])
                                except Exception as ex1:
                                    try:
                                        # We're here because trying to convert to a float failed.
                                        ecsdata[failed_rg_name][field] = local_zone_failed_failed_replication_link_data[field].encode("utf-8")
                                    except Exception as ex2:
                                        pass

                            elif type(local_zone_failed_failed_replication_link_data[field]) is list:
                                logger.debug(MODULE_NAME + '::ecs_collect_local_zone_replication_failure_data()::field from '
                                                            'local_zone_failed_failed_replication_link_data being processed is: ' + field)
                                ecsconnection.get_ecs_detail_data(field=field, metric_list=local_zone_failed_failed_replication_link_data[field],
                                                                metric_values=ecsdata_metrics[failed_rg_name])

                            else:
                                logger.debug(MODULE_NAME + '::ecs_collect_local_zone_replication_failure_data()::field from '
                                                            'local_zone_failed_failed_replication_link_data being processed is: ' + field)
                                ecsconnection.get_ecs_summary_data(field=field, summary_dict=local_zone_failed_failed_replication_link_data[field],
                                                                 current_epoch=current_epoch_time, summary_values=ecsdata_summary[failed_rg_name])

                    for failed_rg_name in ecsdata:
                        db_array = []
                        tags['ReplicationGroupID'] = failed_rg_name
                        db_json = {
                            "measurement": target_name,
                            "tags": tags,
                            "fields": ecsdata[failed_rg_name],
                            "time": current_time
                        }
                        db_array.append(db_json.copy())
                        influxclient.write_points(db_array)
                        logger.debug(MODULE_NAME + '::ecs_collect_local_zone_replication_failure_data()::'
                                                    'Local Zone Failed Replication data db_array is: \r\n\r\n'.join(str(db_array)))

                    for failed_rg_name in ecsdata_metrics:
                        db_array = []
                        tags['ReplicationGroupID'] = failed_rg_name

                        for times in ecsdata_metrics[failed_rg_name]:

                            influxdb_time = datetime.datetime.utcfromtimestamp(int(times))
                            influxdb_time = influxdb_time.strftime("%Y-%m-%dT%H:%M:%S")

                            db_json = {
                                "measurement": target_name+"Metrics",
                                "tags": tags,
                                "fields": ecsdata_metrics[failed_rg_name][times],
                                "time": influxdb_time
                            }
                            db_array.append(db_json.copy())

                        influxclient.write_points(db_array)
                        logger.debug(MODULE_NAME + '::ecs_collect_local_zone_replication_failure_data()::'
                                                    'Local Zone Failed Replication metrics db_array is: \r\n\r\n'.join(str(db_array)))

                    for failed_rg_name in ecsdata_summary:
                        db_array = []
                        tags['ReplicationGroupID'] = failed_rg_name

                        for times in ecsdata_summary[failed_rg_name]:
                            influxdb_time = datetime.datetime.utcfromtimestamp(int(times))
                            influxdb_time = influxdb_time.strftime("%Y-%m-%dT%H:%M:%S")

                            db_json = {
                                "measurement": target_name+"Summary",
                                "tags": tags,
                                "fields": ecsdata_summary[failed_rg_name][times],
                                "time": influxdb_time
                            }
                            db_array.append(db_json.copy())

                        influxclient.write_points(db_array)
                        logger.debug(MODULE_NAME + '::ecs_collect_local_zone_replication_failure_data()::'
                                                   'Local Zone Failed Replication summary '
                                                   'db_array is: \r\n\r\n'.join(str(db_array)))

            if controlledShutdown.kill_now:
                print(MODULE_NAME + "ecs_collect_local_zone_replication_failure_data()::"
                                    "Shutdown detected.  Terminating polling.")
                break

            # Wait for specific polling interval
            time.sleep(float(pollinginterval))
    except Exception as e:
        _logger.error(MODULE_NAME + '::ecs_collect_local_zone_replication_failure_data()::The following unexpected '
                                    'exception occured: ' + str(e) + "\n" + traceback.format_exc())


def ecs_collect_local_zone_bootstrap_data(influxclient, logger, ecsmanagmentapi, pollinginterval):

    try:
        while True:

            # Perform API call against each configured ECS
            for ecsconnection in ecsmanagmentapi:

                # Retrieve local zone bootstrap data via API
                local_zone_bootstrap_data = ecsconnection.get_local_zone_bootstrap_data()

                if local_zone_bootstrap_data is None:
                    logger.error(MODULE_NAME + '::ecs_collect_local_zone_bootstrap_data()::'
                                               'Unable to retrieve ECS Dashboard Local Replication '
                                               'Group Link Bootstrap Information')
                    return
                else:
                    """
                    We have the raw JSON data now lets prep it for Influx
                    """

                    # Declare locals
                    current_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
                    current_epoch_time = time.time()
                    db_array = []
                    ecsdata = {}
                    ecsdata_metrics = {}
                    ecsdata_summary = {}
                    fields = {}
                    tags = {}
                    target_name = "LocalZoneReplicationBootstrap"

                    tags['vdc'] = _ecsVDCLookup.vdc_json[ecsconnection.authentication.host]

                    # Grab just node information
                    replication_link_bootstrap_data = local_zone_bootstrap_data['_embedded']['_instances']

                    # Using 'local_zone_bootstrap_data' so we can re-use code without changing references
                    for local_zone_bootstrap_data in replication_link_bootstrap_data:

                        # Not handling a few metrics for now
                        local_zone_bootstrap_data.pop('_links', None)

                        bootstrap_rg_name = local_zone_bootstrap_data['rgName']
                        ecsdata[bootstrap_rg_name] = {}
                        ecsdata_metrics[bootstrap_rg_name] = {}
                        ecsdata_summary[bootstrap_rg_name] = {}

                        for field in local_zone_bootstrap_data:
                            if type(local_zone_bootstrap_data[field]) is str:
                                try:
                                    logger.debug(MODULE_NAME + '::ecs_collect_local_zone_bootstrap_data()::field from '
                                                                'local_zone_bootstrap_data being processed is: ' + field)
                                    ecsdata[bootstrap_rg_name][field] = float(local_zone_bootstrap_data[field])
                                except Exception as ex1:
                                    try:
                                        # We're here because trying to convert to a float failed.
                                        ecsdata[bootstrap_rg_name][field] = local_zone_bootstrap_data[field].encode("utf-8")
                                    except Exception as ex2:
                                        pass

                            elif type(local_zone_bootstrap_data[field]) is list:
                                logger.debug(MODULE_NAME + '::ecs_collect_local_zone_bootstrap_data()::field from '
                                                            'local_zone_bootstrap_data being processed is: ' + field)
                                ecsconnection.get_ecs_detail_data(field=field, metric_list=local_zone_bootstrap_data[field],
                                                                metric_values=ecsdata_metrics[bootstrap_rg_name])

                            else:
                                logger.debug(MODULE_NAME + '::ecs_collect_local_zone_bootstrap_data()::field from '
                                                            'local_zone_bootstrap_data being processed is: ' + field)
                                ecsconnection.get_ecs_summary_data(field=field, summary_dict=local_zone_bootstrap_data[field],
                                                                 current_epoch=current_epoch_time, summary_values=ecsdata_summary[bootstrap_rg_name])

                    for bootstrap_rg_name in ecsdata:
                        db_array = []
                        tags['ReplicationGroupID'] = bootstrap_rg_name
                        db_json = {
                            "measurement": target_name,
                            "tags": tags,
                            "fields": ecsdata[bootstrap_rg_name],
                            "time": current_time
                        }
                        db_array.append(db_json.copy())
                        influxclient.write_points(db_array)
                        logger.debug(MODULE_NAME + '::ecs_collect_local_zone_bootstrap_data()::'
                                                    'Local Zone Failed Bootstrap data db_array is: \r\n\r\n'.join(str(db_array)))

                    for bootstrap_rg_name in ecsdata_metrics:
                        db_array = []
                        tags['ReplicationGroupID'] = bootstrap_rg_name

                        for times in ecsdata_metrics[bootstrap_rg_name]:

                            influxdb_time = datetime.datetime.utcfromtimestamp(int(times))
                            influxdb_time = influxdb_time.strftime("%Y-%m-%dT%H:%M:%S")

                            db_json = {
                                "measurement": target_name+"Metrics",
                                "tags": tags,
                                "fields": ecsdata_metrics[bootstrap_rg_name][times],
                                "time": influxdb_time
                            }
                            db_array.append(db_json.copy())

                        influxclient.write_points(db_array)
                        logger.debug(MODULE_NAME + '::ecs_collect_local_zone_bootstrap_data()::'
                                                    'Local Zone Failed Bootstrap metrics db_array is: \r\n\r\n'.join(str(db_array)))

                    for bootstrap_rg_name in ecsdata_summary:
                        db_array = []
                        tags['ReplicationGroupID'] = bootstrap_rg_name

                        for times in ecsdata_summary[bootstrap_rg_name]:
                            influxdb_time = datetime.datetime.utcfromtimestamp(int(times))
                            influxdb_time = influxdb_time.strftime("%Y-%m-%dT%H:%M:%S")

                            db_json = {
                                "measurement": target_name+"Summary",
                                "tags": tags,
                                "fields": ecsdata_summary[bootstrap_rg_name][times],
                                "time": influxdb_time
                            }
                            db_array.append(db_json.copy())

                        influxclient.write_points(db_array)
                        logger.debug(MODULE_NAME + '::ecs_collect_local_zone_bootstrap_data()::'
                                                    'Local Zone Failed Bootstrap summary db_array is: \r\n\r\n'.join(str(db_array)))

            if controlledShutdown.kill_now:
                print(MODULE_NAME + "ecs_collect_local_zone_bootstrap_data()::"
                                    "Shutdown detected.  Terminating polling.")
                break

            # Wait for specific polling interval
            time.sleep(float(pollinginterval))
    except Exception as e:
        _logger.error(MODULE_NAME + '::ecs_collect_local_zone_bootstrap_data()::The following unexpected '
                                    'exception occured: ' + str(e) + "\n" + traceback.format_exc())


def ecs_collect_namespace_billing_data(influxclient, logger, ecsmanagmentapi, pollinginterval, tempdir):

    try:
        # Start polling loop
        while True:
            # Perform API call against each configured ECS
            for ecsconnection in ecsmanagmentapi:

                # Retrieve the list of namespaces
                namespace_data = ecsconnection.get_namespace_data()

                if namespace_data is None:
                    logger.info(MODULE_NAME + '::ecs_collect_namespace_billing_data()::'
                                              'Unable to retrieve ECS Namespace Information')
                    return
                else:
                    # For each namespace in the collection get billing retrieve billing information
                    if namespace_data is None:
                        logger.info(MODULE_NAME + '::ecs_collect_namespace_billing_data()::'
                                                  'Unable to retrieve ECS Namespace Information')
                        return
                    else:
                        # Lets set a timestamp that we can use for all data points written during this cycle
                        current_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

                        # Grab the namespace list and cycle thru it
                        if type(namespace_data['namespace']) is list:
                            # Process list of namespaces
                            logger.debug(MODULE_NAME + '::ecs_collect_namespace_billing_data()::'
                                                       'We have the list of namespaces')

                            # For each namespace grab needed info and then grab all the buckets for that namespace
                            for namespace in namespace_data['namespace']:
                                ns_name = namespace['name']
                                ns_id = namespace['id']
                                ns_block_size = namespace['blockSize']
                                ns_notification_size = namespace['notificationSize']
                                ns_total_size_f = 0.0
                                ns_total_objects_f = 0.0
                                ns_total_size_bytes = 0.0
                                ns_total_protected_size_bytes = 0.0

                                if ecs_check_for_integer(ns_block_size):
                                    ns_block_size_int = int(ns_block_size)
                                    if ns_block_size_int > 0:
                                        ns_block_size_bytes = float(ns_block_size) * 1073741824
                                    else:
                                        ns_block_size_bytes = 0
                                        ns_block_size = 0
                                else:
                                    ns_block_size_bytes = 0
                                    ns_block_size = 0

                                if ecs_check_for_integer(ns_notification_size):
                                    ns_notification_size_int = int(ns_notification_size)
                                    if ns_notification_size_int > 0:
                                        ns_notification_size_bytes = float(ns_notification_size) * 1073741824
                                    else:
                                        ns_notification_size_bytes = 0
                                        ns_notification_size = 0
                                else:
                                    ns_notification_size_bytes = 0
                                    ns_notification_size = 0

                                db_array_ns = []
                                ecsdata_ns = {}
                                fields_ns = {}
                                tags_ns = {}
                                target_name_ns = "metering_stats_namespace"

                                # Retrieve the list of buckets for the namespace
                                bucket_data = ecsconnection.get_bucket_data(ns_name)

                                if bucket_data is None:
                                    logger.info(MODULE_NAME + '::ecs_collect_namespace_billing_data()::'
                                                              'Unable to retrieve the list of buckets for namespace ' + ns_name)
                                    return
                                else:
                                    # We've got some data bucket data lets
                                    # check if we have some buckets for this namespace to process

                                    for bucket in bucket_data['object_bucket']:
                                        bucket_name = bucket['name']
                                        soft_quota = bucket['softquota']
                                        block_size = bucket['block_size']
                                        notification_size = bucket['notification_size']

                                        # Lets calculate quota sizes in bytes if provided -
                                        # Quota Values are always set in GiB on ECS but we want them in bytes
                                        if ecs_check_for_integer(soft_quota):
                                            soft_quota_int = int(soft_quota)
                                            if soft_quota_int > 0:
                                                soft_quota_size_bytes = float(soft_quota) * 1073741824
                                            else:
                                                soft_quota_size_bytes = 0
                                                soft_quota = 0
                                        else:
                                            soft_quota_size_bytes = 0
                                            soft_quota = 0

                                        if ecs_check_for_integer(block_size):
                                            block_size_int = int(block_size)
                                            if block_size_int > 0:
                                                block_size_bytes = float(block_size) * 1073741824
                                            else:
                                                block_size_bytes = 0
                                                block_size = 0
                                        else:
                                            block_size_bytes = 0
                                            block_size = 0

                                        if ecs_check_for_integer(notification_size):
                                            notification_size_int = int(notification_size)
                                            if notification_size_int < 0:
                                                notification_size = 0
                                        else:
                                            notification_size = 0

                                        # We have a namespace and a bucket lets go
                                        # and retrieve the metering information
                                        billing_data_file = ecsconnection.get_namespace_billing_data(ns_name, bucket_name, tempdir)

                                        if billing_data_file is None:
                                            # If we had an issue just log the error and keep going to the next bucket
                                            logger.info(MODULE_NAME + '::ecs_collect_namespace_billing_data()::'
                                                                      'Unable to retrieve Metering information for ' + ns_name + ' and bucket ' + bucket_name)
                                        else:
                                            # We have a metering file for the bucket and namespace so lets
                                            # parse it and create an InfluxDB datapoint
                                            try:
                                                tree = ET.parse(billing_data_file)
                                                billing_info = tree.getroot()

                                                # Grab VDC Name
                                                vdc = _ecsVDCLookup.vdc_json[ecsconnection.authentication.host]
                                                managementIp = ecsconnection.authentication.host

                                                vpool_id = billing_info.find('vpool_id').text
                                                total_size = billing_info.find('total_size').text
                                                total_objects = billing_info.find('total_objects').text

                                                # No need to close the file as the ET parse()
                                                # method will close it when parsing is completed.
                                                _logger.debug(MODULE_NAME + '::ecs_collect_namespace_billing_data::Deleting temporary '
                                                                            'xml file: ' + billing_data_file)

                                                # We have parsed our metering file for
                                                # the current bucket now lets create a data point
                                                current_epoch_time = time.time()
                                                db_array = []
                                                ecsdata = {}
                                                fields = {}
                                                tags = {}
                                                target_name = "metering_stats"

                                                # Setup measurement tags.
                                                tags['vdc'] = _ecsVDCLookup.vdc_json[ecsconnection.authentication.host]
                                                tags['namespace'] = ns_name
                                                tags['bucket'] = bucket_name
                                                # tags['virtual_pool_id'] = vpool_id

                                                # We always grab capacity data in KB and we want to convert it to bytes
                                                total_size_f = float(total_size)
                                                total_size_bytes = total_size_f * 1024.00
                                                total_protected_size_bytes = total_size_bytes * 1.33

                                                # Calculate average object sizes if
                                                # objects and size are greater than zero
                                                total_objects_f = float(total_objects)
                                                if total_objects_f > 0:
                                                    if total_size_f > 0:
                                                        average_object_size_f = total_size_bytes / total_objects_f
                                                    else:
                                                        average_object_size_f = 0.0
                                                else:
                                                    average_object_size_f = 0.0

                                                # If we have hard and / or soft quotas
                                                # lets calculate quota utilization %
                                                if soft_quota_size_bytes > 0:
                                                    if total_size_bytes > 0:
                                                        soft_quota_utilization = total_size_bytes / soft_quota_size_bytes
                                                        soft_quota_utilization_protected = total_protected_size_bytes / soft_quota_size_bytes
                                                    else:
                                                        soft_quota_utilization = 0
                                                        soft_quota_utilization_protected = 0
                                                else:
                                                    soft_quota_utilization = 0
                                                    soft_quota_utilization_protected = 0

                                                if block_size_bytes > 0:
                                                    if total_size_bytes > 0:
                                                        hard_quota_utilization = total_size_bytes / block_size_bytes
                                                        hard_quota_utilization_protected = total_protected_size_bytes / block_size_bytes
                                                    else:
                                                        hard_quota_utilization = 0
                                                        hard_quota_utilization_protected = 0
                                                else:
                                                    hard_quota_utilization = 0
                                                    hard_quota_utilization_protected = 0

                                                # Add bucket level details to namespace totals
                                                ns_total_size_f += total_size_f
                                                ns_total_objects_f += total_objects_f
                                                ns_total_size_bytes += total_size_bytes
                                                ns_total_protected_size_bytes += total_protected_size_bytes

                                                # Load dictionary of values
                                                ecsdata[bucket_name] = {}
                                                try:
                                                    ecsdata[bucket_name]['total_size'] = float(total_size_bytes)
                                                except Exception as ex1:
                                                    try:
                                                        # We're here because trying to convert to a float failed.
                                                        ecsdata[bucket_name]['total_size'] = total_size_bytes
                                                    except Exception as ex2:
                                                        pass

                                                try:
                                                    ecsdata[bucket_name]['total_objects'] = float(total_objects)
                                                except Exception as ex3:
                                                    try:
                                                        # We're here because trying to convert to a float failed.
                                                        ecsdata[bucket_name]['total_objects'] = total_objects
                                                    except Exception as ex4:
                                                        pass

                                                try:
                                                    ecsdata[bucket_name]['soft_quota'] = float(soft_quota)
                                                except Exception as ex5:
                                                    try:
                                                        # We're here because trying to convert to a float failed.
                                                        ecsdata[bucket_name]['soft_quota'] = soft_quota
                                                    except Exception as ex6:
                                                        pass

                                                try:
                                                    ecsdata[bucket_name]['hard_quota'] = float(block_size)
                                                except Exception as ex7:
                                                    try:
                                                        # We're here because trying to convert to a float failed.
                                                        ecsdata[bucket_name]['hard_quota'] = block_size
                                                    except Exception as ex8:
                                                        pass

                                                try:
                                                    ecsdata[bucket_name]['notification_size'] = float(notification_size)
                                                except Exception as ex7:
                                                    try:
                                                        # We're here because trying to convert to a float failed.
                                                        ecsdata[bucket_name]['notification_size'] = notification_size
                                                    except Exception as ex8:
                                                        pass

                                                try:
                                                    ecsdata[bucket_name]['average_size'] = float(average_object_size_f)
                                                except Exception as ex7:
                                                    try:
                                                        # We're here because trying to convert to a float failed.
                                                        ecsdata[bucket_name]['average_size'] = average_object_size_f
                                                    except Exception as ex8:
                                                        pass

                                                try:
                                                    ecsdata[bucket_name]['soft_quota_utilization'] = float(soft_quota_utilization)
                                                except Exception as ex7:
                                                    try:
                                                        # We're here because trying to convert to a float failed.
                                                        ecsdata[bucket_name]['soft_quota_utilization'] = soft_quota_utilization
                                                    except Exception as ex8:
                                                        pass

                                                try:
                                                    ecsdata[bucket_name]['hard_quota_utilization'] = float(hard_quota_utilization)
                                                except Exception as ex7:
                                                    try:
                                                        # We're here because trying to convert to a float failed.
                                                        ecsdata[bucket_name]['hard_quota_utilization'] = hard_quota_utilization
                                                    except Exception as ex8:
                                                        pass

                                                try:
                                                    ecsdata[bucket_name]['total_protected_size'] = float(total_protected_size_bytes)
                                                except Exception as ex7:
                                                    try:
                                                        # We're here because trying to convert to a float failed.
                                                        ecsdata[bucket_name]['total_protected_size'] = total_protected_size_bytes
                                                    except Exception as ex8:
                                                        pass

                                                try:
                                                    ecsdata[bucket_name]['soft_quota_utilization_protected'] = float(soft_quota_utilization_protected)
                                                except Exception as ex7:
                                                    try:
                                                        # We're here because trying to convert to a float failed.
                                                        ecsdata[bucket_name]['soft_quota_utilization_protected'] = soft_quota_utilization_protected
                                                    except Exception as ex8:
                                                        pass

                                                try:
                                                    ecsdata[bucket_name]['hard_quota_utilization_protected'] = float(hard_quota_utilization_protected)
                                                except Exception as ex7:
                                                    try:
                                                        # We're here because trying to convert to a float failed.
                                                        ecsdata[bucket_name]['hard_quota_utilization_protected'] = hard_quota_utilization_protected
                                                    except Exception as ex8:
                                                        pass

                                                # Create Influx DB Info Dictionary for
                                                # our string fields and add it to the db list
                                                db_json = {
                                                    "measurement": target_name,
                                                    "tags": tags,
                                                    "fields": ecsdata[bucket_name],
                                                    "time": current_time
                                                }
                                                db_array.append(db_json.copy())

                                                # Write data to Influx
                                                influxclient.write_points(db_array)

                                                # Dump array for debug
                                                logger.debug(MODULE_NAME + '::ecs_collect_namespace_billing_data()::'
                                                                           'Billing db_array is: \r\n\r\n'.join(str(db_array)))

                                                # Delete temp file
                                                ecs_delete_file(billing_data_file)

                                            except Exception as ex:
                                                logger.error(MODULE_NAME + '::ecs_collect_namespace_billing_data()::The following unexpected '
                                                                           'exception occurred: ' + str(ex) + "\n" + traceback.format_exc())
                                # Let log namespace level info
                                tags_ns['vdc'] = _ecsVDCLookup.vdc_json[ecsconnection.authentication.host]
                                tags_ns['namespace'] = ns_name
                                ecsdata_ns[ns_name] = {}

                                # Calculate average object sizes for the namespace if
                                # objects and size are greater than zero
                                if ns_total_objects_f > 0:
                                    if ns_total_size_f > 0:
                                        ns_average_object_size_f = ns_total_size_f / ns_total_objects_f
                                    else:
                                        ns_average_object_size_f = 0.0
                                else:
                                    ns_average_object_size_f = 0.0

                                # If we have hard and / or soft quotas
                                # lets calculate quota utilization %
                                if ns_notification_size_bytes > 0:
                                    if ns_total_size_bytes > 0:
                                        ns_soft_quota_utilization = ns_total_size_bytes / ns_notification_size_bytes
                                        ns_soft_quota_utilization_protected = ns_total_protected_size_bytes / ns_notification_size_bytes
                                    else:
                                        ns_soft_quota_utilization = 0
                                        ns_soft_quota_utilization_protected = 0
                                else:
                                    ns_soft_quota_utilization = 0
                                    ns_soft_quota_utilization_protected = 0

                                if ns_block_size_bytes > 0:
                                    if ns_total_size_bytes > 0:
                                        ns_hard_quota_utilization = ns_total_size_bytes / ns_block_size_bytes
                                        ns_hard_quota_utilization_protected = ns_total_protected_size_bytes / ns_block_size_bytes
                                    else:
                                        ns_hard_quota_utilization = 0
                                        ns_hard_quota_utilization_protected = 0
                                else:
                                    ns_hard_quota_utilization = 0
                                    ns_hard_quota_utilization_protected = 0

                                # Add namespace values to the array for writing to Influx
                                try:
                                    ecsdata_ns[ns_name]['ns_average_size'] = float(ns_average_object_size_f)
                                except Exception as ex5:
                                    try:
                                        # We're here because trying to convert to a float failed.
                                        ecsdata_ns[ns_name]['ns_average_size'] = ns_average_object_size_f
                                    except Exception as ex6:
                                        pass

                                try:
                                    ecsdata_ns[ns_name]['ns_hard_quota'] = float(ns_block_size)
                                except Exception as ex5:
                                    try:
                                        # We're here because trying to convert to a float failed.
                                        ecsdata_ns[ns_name]['ns_hard_quota'] = ns_block_size
                                    except Exception as ex6:
                                        pass

                                try:
                                    ecsdata_ns[ns_name]['ns_soft_quota'] = float(ns_notification_size)
                                except Exception as ex7:
                                    try:
                                        # We're here because trying to convert to a float failed.
                                        ecsdata_ns[ns_name]['ns_soft_quota'] = ns_notification_size
                                    except Exception as ex8:
                                        pass

                                try:
                                    ecsdata_ns[ns_name]['ns_total_size'] = float(ns_total_size_bytes)
                                except Exception as ex1:
                                    try:
                                        # We're here because trying to convert to a float failed.
                                        ecsdata_ns[ns_name]['ns_total_size'] = ns_total_size_bytes
                                    except Exception as ex2:
                                        pass

                                try:
                                    ecsdata_ns[ns_name]['ns_total_objects'] = float(ns_total_objects_f)
                                except Exception as ex1:
                                    try:
                                        # We're here because trying to convert to a float failed.
                                        ecsdata_ns[ns_name]['ns_total_objects'] = ns_total_objects_f
                                    except Exception as ex2:
                                        pass

                                try:
                                    ecsdata_ns[ns_name]['ns_total_protected_size'] = float(ns_total_protected_size_bytes)
                                except Exception as ex1:
                                    try:
                                        # We're here because trying to convert to a float failed.
                                        ecsdata_ns[ns_name]['ns_total_protected_size'] = ns_total_protected_size_bytes
                                    except Exception as ex2:
                                        pass

                                try:
                                    ecsdata_ns[ns_name]['ns_soft_quota_utilization'] = float(ns_soft_quota_utilization)
                                except Exception as ex1:
                                    try:
                                        # We're here because trying to convert to a float failed.
                                        ecsdata_ns[ns_name]['ns_soft_quota_utilization'] = ns_soft_quota_utilization
                                    except Exception as ex2:
                                        pass

                                try:
                                    ecsdata_ns[ns_name]['ns_hard_quota_utilization'] = float(ns_hard_quota_utilization)
                                except Exception as ex1:
                                    try:
                                        # We're here because trying to convert to a float failed.
                                        ecsdata_ns[ns_name]['ns_hard_quota_utilization'] = ns_hard_quota_utilization
                                    except Exception as ex2:
                                        pass

                                try:
                                    ecsdata_ns[ns_name]['ns_soft_quota_utilization_protected'] = float(ns_soft_quota_utilization_protected)
                                except Exception as ex1:
                                    try:
                                        # We're here because trying to convert to a float failed.
                                        ecsdata_ns[ns_name]['ns_soft_quota_utilization_protected'] = ns_soft_quota_utilization_protected
                                    except Exception as ex2:
                                        pass

                                try:
                                    ecsdata_ns[ns_name]['ns_hard_quota_utilization_protected'] = float(ns_hard_quota_utilization_protected)
                                except Exception as ex1:
                                    try:
                                        # We're here because trying to convert to a float failed.
                                        ecsdata_ns[ns_name]['ns_hard_quota_utilization_protected'] = ns_hard_quota_utilization_protected
                                    except Exception as ex2:
                                        pass
                                # Create Influx DB Info Dictionary for
                                # our string fields and add it to the db list
                                db_json_ns = {
                                    "measurement": target_name_ns,
                                    "tags": tags_ns,
                                    "fields": ecsdata_ns[ns_name],
                                    "time": current_time
                                }
                                db_array_ns.append(db_json_ns.copy())

                                # Write data to Influx
                                influxclient.write_points(db_array_ns)

                                # Dump array for debug
                                logger.debug(MODULE_NAME + '::ecs_collect_namespace_billing_data()::'
                                                           'Namespace Billing db_array is: \r\n\r\n'.join(str(db_array_ns)))

                        else:
                            # We should have found the namespace list in the dictionary.  We have an issue
                            logger.info(MODULE_NAME + '::ecs_collect_namespace_billing_data()::'
                                                      'Unable to retrieve the list of namespaces '
                                                      'from the namespace data dictionary.')
                            return


            if controlledShutdown.kill_now:
                print(MODULE_NAME + "ecs_collect_namespace_billing_data()::Shutdown detected.  Terminating polling.")
                break

            # Wait for specific polling interval
            time.sleep(float(pollinginterval))
    except Exception as e:
        _logger.error(MODULE_NAME + '::ecs_collect_namespace_billing_data()::The following unexpected '
                                    'exception occured: ' + str(e) + "\n" + traceback.format_exc())


def ecs_authenticate():
    global _ecsAuthentication
    global _configuration
    global _logger
    global _ecsManagmentAPI
    connected = True

    try:
        # Wait till configuration is set
        while not _configuration:
            time.sleep(1)

        # Iterate over all ECS Connections configured and attempt tp Authenticate to ECS
        for ecsconnection in _configuration.ecsconnections:

            # Attempt to authenticate
            auth = ECSAuthentication(ecsconnection['protocol'], ecsconnection['host'], ecsconnection['user'],
                                     ecsconnection['password'], ecsconnection['port'], _logger)

            auth.connect()

            # Check to see if we have a token returned
            if auth.token is None:
                _logger.error(MODULE_NAME + '::ecs_init()::Unable to authenticate to ECS as configured.  '
                             'Please validate and try again.')
                connected = False
                break
            else:
                _ecsAuthentication.append(auth)

                # Instantiate ECS Management API object, add it to our list, and validate that we are authenticated
                _ecsManagmentAPI.append(ECSManagementAPI(auth, ecsconnection['connectTimeout'],
                                                         ecsconnection['readTimeout'], _logger))
                if not _ecsAuthentication:
                    _logger.info(MODULE_NAME + '::ecs_authenticate()::ECS Data Collection '
                                               'Module is not ready.  Please check logs.')
                    connected = False
                    break

        return connected

    except Exception as e:
        _logger.error(MODULE_NAME + '::ecs_init()::Cannot initialize plugin. Cause: '
                      + str(e) + "\n" + traceback.format_exc())
        connected = False


def influx_init():
    global _influxClient
    global _configuration
    global _logger
    connected = True

    try:
        # Wait till configuration is set
        while not _configuration:
            time.sleep(1)

        # Instantiate utility object and check to see if our database exists
        db_utility = InfluxUtility(_configuration, _logger)
        database_found = db_utility.check_db_exists(_configuration.database_name)

        # If database is not found then connect with no database, create the database,
        # add a retention policy, and then switch to it
        if not database_found:
            # Create a client connection to the Influx Server
            influx_client = InfluxDBClient(_configuration.database_host, _configuration.database_port,
                                           _configuration.database_user, _configuration.database_password, None)

            # Create the database and switch to it
            influx_client.create_database(_configuration.database_name)
            influx_client.switch_database(_configuration.database_name)

            # Drop autogen retention policy
            influx_client.drop_retention_policy('autogen', database=_configuration.database_name)

            # Create and assign default retention policy
            influx_client.create_retention_policy(_configuration.database_retentionPolicyName,
                                                  _configuration.database_retentionPolicyDuration,
                                                  _configuration.database_retentionPolicyReplicationFactor,
                                                  database=_configuration.database_name, default=True)

            # Create default retention policy
            influx_client.create_retention_policy(_configuration.database_retentionPolicyName,
                                                  _configuration.database_retentionPolicyDuration,
                                                  _configuration.database_retentionPolicyReplicationFactor,
                                                  database=_configuration.database_name, default=True)

        else:
            # Connect to influx with existing database
            influx_client = InfluxDBClient(_configuration.database_host, _configuration.database_port, _configuration.database_user, _configuration.database_password,_configuration.database_name)

        if influx_client is None:
            _logger.error(MODULE_NAME + '::influx_init()::Unable to connect to Influx as configured.  '
                                       'Please validate and try again.')
            connected = False
        else:
            _logger.info(MODULE_NAME + '::influx_init()::Successfully connected to Influx as configured.')
            _influxClient = influx_client

        return connected

    except Exception as e:
        _logger.error(MODULE_NAME + '::influx_init()::Cannot initialize Influx connection. Cause: '
                      + str(e) + "\n" + traceback.format_exc())
        connected = False


def ecs_data_collection():
    global _influxClient
    global _ecsAuthentication
    global _logger
    global _ecsManagmentAPI

    try:
        # Wait till configuration is set
        while not _configuration:
            time.sleep(1)

        # Now lets spin up a thread for each API call with it's own custom polling interval by iterating
        # through our module configuration
        for i, j in _configuration.modules_intervals.items():
            method = str(i)
            interval = str(j)
            t = ECSDataCollection(method, _influxClient, _logger, _ecsManagmentAPI, interval, _configuration.tempfilepath)
            t.start()

    except Exception as e:
        _logger.error(MODULE_NAME + '::ecs_data_collection()::A failure ocurred during data collection. Cause: '
                      + str(e) + "\n" + traceback.format_exc())


"""
Main 
"""
if __name__ == "__main__":

    try:
        # Create object to support controlled shutdown
        controlledShutdown = ECSDataCollectionShutdown()

        # Dump out application path
        currentApplicationDirectory = os.getcwd()
        configFilePath = os.path.abspath(os.path.join(currentApplicationDirectory, "configuration", CONFIG_FILE))
        vdcLookupFilePath = os.path.abspath(os.path.join(currentApplicationDirectory, "configuration", VDC_LOOKUP_FILE))
        tempFilePath = os.path.abspath(os.path.join(currentApplicationDirectory, "temp"))

        # Create temp diretory if it doesn't already exists
        if not os.path.isdir(tempFilePath):
            os.mkdir(tempFilePath)
        else:
            # The directory exists so lets scrub any temp XML files out that may be in there
            files = os.listdir(tempFilePath)
            for file in files:
                if file.endswith(".xml"):
                    os.remove(os.path.join(currentApplicationDirectory, "temp", file))

        print(MODULE_NAME + "::__main__::Current directory is : " + currentApplicationDirectory)
        print(MODULE_NAME + "::__main__::Configuration file path is: " + configFilePath)

        # Initialize configuration and VDC Lookup
        ecs_config(configFilePath, vdcLookupFilePath, tempFilePath)

        # Initialize connection(s) to ECS
        if ecs_authenticate():

            # Initialize database connection
            if influx_init():

                # Launch ECS Data Collection polling threads
                ecs_data_collection()

                # Check for shutdown
                if controlledShutdown.kill_now:
                    print(MODULE_NAME + "__main__::Controlled shutdown completed.")
    except Exception as e:
        print(MODULE_NAME + '__main__::The following unexpected error occured: '
              + str(e) + "\n" + traceback.format_exc())

