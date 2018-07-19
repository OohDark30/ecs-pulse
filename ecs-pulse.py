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
import datetime
import os
import traceback
import signal
import time
import logging

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
_ecsAuthentication = None
_influxClient = None
_ecsVDCLookup = None

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


def ecs_config(config, vdc_config):
    global _configuration
    global _logger
    global _ecsAuthentication
    global _ecsVDCLookup

    # Load and validate module configuration
    _configuration = ECSPulseConfiguration(config)

    # Load ECS VDC Lookup
    _ecsVDCLookup = ECSUtility(_ecsAuthentication, _logger, vdc_config)

    # Grab loggers and log status
    _logger = ecs_logger.get_logger(__name__, _configuration.logging_level)
    _logger.info(MODULE_NAME + '::ecs_config()::We have configured logging level to: '
                 + logging.getLevelName(str(_configuration.logging_level)))
    _logger.info(MODULE_NAME + '::ecs_config()::Configuring ECS Data Collection Module complete.')


def ecs_collect_data():
    global _influxClient
    global _ecsAuthentication
    global _logger

    # Instantiate ECS Management API object and validate that we are authenticated
    ecsmgmntapi = ECSManagementAPI(_ecsAuthentication, _logger)
    if not _ecsAuthentication:
        _logger.info(MODULE_NAME + '::ecs_collect_data()::ECS Data Collection Module is not ready.  Please check logs.')
        return

    # Retrieve local zone data via API
    local_zone_data = ecsmgmntapi.get_local_zone_data()

    if local_zone_data is None:
        _logger.error(MODULE_NAME + '::ecs_collect_data()::Unable to retrieve ECS Dashboard Local Zone Information')
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
        target_name = "LocalZone"

        # Remove data points from raw json we are not interested in
        local_zone_data.pop('_links', None)
        local_zone_data.pop('transactionErrors', None)
        local_zone_data.pop('transactionErrorsSummary', None)
        local_zone_data.pop('transactionErrorsCurrent', None)

        # Grab VDC Name
        tags['VDC'] = _ecsVDCLookup.vdc_json[_ecsAuthentication.host]

        # Process remaining data in JSON
        for field in local_zone_data:
            # Process individual data field
            if type(local_zone_data[field]) is unicode:
                try:
                    _logger.debug(MODULE_NAME + '::ecs_collect_data()::field from local_zone_data being processed is: ' + field)
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
                _logger.debug(MODULE_NAME + '::ecs_collect_data()::field from local_zone_data being processed is: ' + field)
                ecsmgmntapi.get_ecs_detail_data(field=field, metric_list=local_zone_data[field], metric_values=ecsdata_metrics)
            else:
                # Process dictionary fields
                _logger.debug(MODULE_NAME + '::ecs_collect_data()::field from local_zone_data being processed is: ' + field)
                ecsmgmntapi.get_ecs_summary_data(field=field, summary_dict=local_zone_data[field],
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
        _influxClient.write_points(db_array)

        # Dump array for debug
        _logger.debug(MODULE_NAME + '::ecs_collect_data()::Local Zone db_array is: \r\n\r\n'.join(str(db_array)))

    # Retrieve capacity data via API
    capacity_data = ecsmgmntapi.get_capacity_data()

    if capacity_data is None:
        _logger.info(MODULE_NAME + '::ecs_collect_data()::Unable to retrieve ECS Dashboard Capacity Information')
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
        tags['VDC'] = _ecsVDCLookup.vdc_json[_ecsAuthentication.host]

        # Process remaining data in JSON
        for field in capacity_data:
            # Process individual data field
            if type(capacity_data[field]) is int:
                try:
                    _logger.debug(MODULE_NAME + '::ecs_collect_data()::field from capacity_data being processed is: ' + field)
                    ecsdata[field] = float(capacity_data[field])
                except Exception:
                    try:
                        # We're here because trying to convert to a float failed.  Store whatever value is there
                        ecsdata[field] = local_zone_data[field].encode("utf-8")
                    except Exception:
                        pass
            # Process list fields
            elif type(capacity_data[field]) is list:
                _logger.debug(MODULE_NAME + '::ecs_collect_data()::field from capacity_data being processed is: ' + field)
                ecsmgmntapi.get_ecs_detail_data(field=field, metric_list=capacity_data[field], metric_values=ecsdata_metrics)
            else:
                # Process dictionary fields
                _logger.debug(MODULE_NAME + '::ecs_collect_data()::field from capacity_data being processed is: ' + field)
                ecsmgmntapi.get_ecs_summary_data(field=field, summary_dict=capacity_data[field],
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
        _influxClient.write_points(db_array)

        # Dump array for debug
        _logger.debug(MODULE_NAME + '::ecs_collect_data()::Capacity db_array is: \r\n\r\n'.join(str(db_array)))

    # Retrieve local zone replication data via API
    local_zone_replication_data = ecsmgmntapi.get_local_zone_replication_data()

    if local_zone_replication_data is None:
        _logger.error(MODULE_NAME + '::ecs_collect_data()::Unable to retrieve ECS Dashboard Local Replication Node Information')
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

        tags['VDC'] = _ecsVDCLookup.vdc_json[_ecsAuthentication.host]

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
                if type(local_zone_replication_data[field]) is unicode:
                    try:
                        _logger.debug(MODULE_NAME + '::ecs_collect_data()::field from '
                                                    'local_zone_replication_data being processed is: ' + field)
                        ecsdata[node_name][field] = float(local_zone_replication_data[field])
                    except Exception as ex1:
                        try:
                            # We're here because trying to convert to a float failed.
                            ecsdata[node_name][field] = local_zone_replication_data[field].encode("utf-8")
                        except Exception as ex2:
                            pass

                elif type(local_zone_replication_data[field]) is list:
                    _logger.debug(MODULE_NAME + '::ecs_collect_data()::field from '
                                                'local_zone_replication_data being processed is: ' + field)
                    ecsmgmntapi.get_ecs_detail_data(field=field, metric_list=local_zone_replication_data[field],
                                                    metric_values=ecsdata_metrics[node_name])

                else:
                    _logger.debug(MODULE_NAME + '::ecs_collect_data()::field from '
                                                'local_zone_replication_data being processed is: ' + field)
                    ecsmgmntapi.get_ecs_summary_data(field=field, summary_dict=local_zone_replication_data[field],
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
            _influxClient.write_points(db_array)

            # Dump array for debug
            _logger.debug(MODULE_NAME + '::ecs_collect_data()::Local Zone Replication field db_array is: \r\n\r\n'.join(str(db_array)))

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
            _influxClient.write_points(db_array)

            # Dump array for debug
            _logger.debug(MODULE_NAME + '::ecs_collect_data()::Local Zone Replication metrics db_array is: \r\n\r\n'.join(str(db_array)))

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

            _influxClient.write_points(db_array)

            # Dump array for debug
            _logger.debug(MODULE_NAME + '::ecs_collect_data()::Local Zone Replication summary db_array is: \r\n\r\n'.join(str(db_array)))

    # Retrieve local zone failed replication data via API
    local_zone_failed_failed_replication_link_data = ecsmgmntapi.get_local_zone_replication_failure_data()

    if local_zone_failed_failed_replication_link_data is None:
        _logger.error(MODULE_NAME + '::ecs_collect_data()::Unable to retrieve ECS Dashboard Local Replication Group Link Failure Information')
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

        tags['VDC'] = _ecsVDCLookup.vdc_json[_ecsAuthentication.host]

        # Grab just node information
        failed_replication_link_data = local_zone_failed_failed_replication_link_data['_embedded']['_instances']

        # Using 'local_zone_failed_failed_replication_link_data' so we can re-use code without changing references
        for local_zone_failed_failed_replication_link_data in failed_replication_link_data:

            # Not handling a few metrics for now
            local_zone_failed_failed_replication_link_data.pop('_links', None)

            failed_rg_name = local_zone_failed_failed_replication_link_data['rgName']
            ecsdata[failed_rg_name] = {}
            ecsdata_metrics[failed_rg_name] = {}
            ecsdata_summary[failed_rg_name] = {}

            for field in local_zone_failed_failed_replication_link_data:
                if type(local_zone_failed_failed_replication_link_data[field]) is unicode:
                    try:
                        _logger.debug(MODULE_NAME + '::ecs_collect_data()::field from '
                                                    'local_zone_failed_failed_replication_link_data being processed is: ' + field)
                        ecsdata[failed_rg_name][field] = float(local_zone_failed_failed_replication_link_data[field])
                    except Exception as ex1:
                        try:
                            # We're here because trying to convert to a float failed.
                            ecsdata[failed_rg_name][field] = local_zone_failed_failed_replication_link_data[field].encode("utf-8")
                        except Exception as ex2:
                            pass

                elif type(local_zone_failed_failed_replication_link_data[field]) is list:
                    _logger.debug(MODULE_NAME + '::ecs_collect_data()::field from '
                                                'local_zone_failed_failed_replication_link_data being processed is: ' + field)
                    ecsmgmntapi.get_ecs_detail_data(field=field, metric_list=local_zone_failed_failed_replication_link_data[field],
                                                    metric_values=ecsdata_metrics[failed_rg_name])

                else:
                    _logger.debug(MODULE_NAME + '::ecs_collect_data()::field from '
                                                'local_zone_failed_failed_replication_link_data being processed is: ' + field)
                    ecsmgmntapi.get_ecs_summary_data(field=field, summary_dict=local_zone_failed_failed_replication_link_data[field],
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
            _influxClient.write_points(db_array)
            _logger.debug(MODULE_NAME + '::ecs_collect_data()::Local Zone Failed Replication data db_array is: \r\n\r\n'.join(str(db_array)))

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

            _influxClient.write_points(db_array)
            _logger.debug(MODULE_NAME + '::ecs_collect_data()::Local Zone Failed Replication metrics db_array is: \r\n\r\n'.join(str(db_array)))

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

            _influxClient.write_points(db_array)
            _logger.debug(MODULE_NAME + '::ecs_collect_data()::Local Zone Failed Replication summary db_array is: \r\n\r\n'.join(str(db_array)))

    # Retrieve local zone bootstrap data via API
    local_zone_bootstrap_data = ecsmgmntapi.get_local_zone_bootstrap_data()

    if local_zone_bootstrap_data is None:
        _logger.error(MODULE_NAME + '::ecs_collect_data()::Unable to retrieve ECS Dashboard Local Replication Group Link Bootstrap Information')
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

        tags['VDC'] = _ecsVDCLookup.vdc_json[_ecsAuthentication.host]

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
                if type(local_zone_bootstrap_data[field]) is unicode:
                    try:
                        _logger.debug(MODULE_NAME + '::ecs_collect_data()::field from '
                                                    'local_zone_bootstrap_data being processed is: ' + field)
                        ecsdata[bootstrap_rg_name][field] = float(local_zone_bootstrap_data[field])
                    except Exception as ex1:
                        try:
                            # We're here because trying to convert to a float failed.
                            ecsdata[bootstrap_rg_name][field] = local_zone_bootstrap_data[field].encode("utf-8")
                        except Exception as ex2:
                            pass

                elif type(local_zone_bootstrap_data[field]) is list:
                    _logger.debug(MODULE_NAME + '::ecs_collect_data()::field from '
                                                'local_zone_bootstrap_data being processed is: ' + field)
                    ecsmgmntapi.get_ecs_detail_data(field=field, metric_list=local_zone_bootstrap_data[field],
                                                    metric_values=ecsdata_metrics[bootstrap_rg_name])

                else:
                    _logger.debug(MODULE_NAME + '::ecs_collect_data()::field from '
                                                'local_zone_bootstrap_data being processed is: ' + field)
                    ecsmgmntapi.get_ecs_summary_data(field=field, summary_dict=local_zone_bootstrap_data[field],
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
            _influxClient.write_points(db_array)
            _logger.debug(MODULE_NAME + '::ecs_collect_data()::Local Zone Failed Bootstrap data db_array is: \r\n\r\n'.join(str(db_array)))

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

            _influxClient.write_points(db_array)
            _logger.debug(MODULE_NAME + '::ecs_collect_data()::Local Zone Failed Bootstrap metrics db_array is: \r\n\r\n'.join(str(db_array)))

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

            _influxClient.write_points(db_array)
            _logger.debug(MODULE_NAME + '::ecs_collect_data()::Local Zone Failed Bootstrap summary db_array is: \r\n\r\n'.join(str(db_array)))

    # Retrieve local zone disk data via API
    local_zone_disk_data = ecsmgmntapi.get_local_zone_disk_data()

    if local_zone_disk_data is None:
        _logger.error(MODULE_NAME + '::ecs_collect_data()::Unable to retrieve ECS Dashboard Local Zone Disk Information')
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

        tags['VDC'] = _ecsVDCLookup.vdc_json[_ecsAuthentication.host]

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
                if type(local_zone_disk_data[field]) is unicode:
                    try:
                        _logger.debug(MODULE_NAME + '::ecs_collect_data()::field from '
                                                    'local_zone_disk_data being processed is: ' + field)
                        ecsdata[disk_display_name][field] = float(local_zone_disk_data[field])
                    except Exception as ex1:
                        try:
                            # We're here because trying to convert to a float failed.
                            ecsdata[disk_display_name][field] = local_zone_disk_data[field].encode("utf-8")
                        except Exception as ex2:
                            pass

                elif type(local_zone_disk_data[field]) is list:
                    _logger.debug(MODULE_NAME + '::ecs_collect_data()::field from '
                                                'local_zone_disk_data being processed is: ' + field)
                    ecsmgmntapi.get_ecs_detail_data(field=field, metric_list=local_zone_disk_data[field],
                                                    metric_values=ecsdata_metrics[disk_display_name])

                else:
                    _logger.debug(MODULE_NAME + '::ecs_collect_data()::field from '
                                                'local_zone_disk_data being processed is: ' + field)
                    ecsmgmntapi.get_ecs_summary_data(field=field, summary_dict=local_zone_disk_data[field],
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
            _influxClient.write_points(db_array)
            _logger.debug(MODULE_NAME + '::ecs_collect_data()::Local Zone Failed Disk data db_array is: \r\n\r\n'.join(str(db_array)))

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

            _influxClient.write_points(db_array)
            _logger.debug(MODULE_NAME + '::ecs_collect_data()::Local Zone Failed Disk metrics db_array is: \r\n\r\n'.join(str(db_array)))

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

            _influxClient.write_points(db_array)
            _logger.debug(MODULE_NAME + '::ecs_collect_data()::Local Zone Failed Disk summary db_array is: \r\n\r\n'.join(str(db_array)))

    # Retrieve local zone node data via API
    local_zone_node_data = ecsmgmntapi.get_local_zone_node_data()

    if local_zone_node_data is None:
        _logger.error(MODULE_NAME + '::ecs_collect_data()::Unable to retrieve ECS Dashboard Local Zone Node Information')
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

        tags['VDC'] = _ecsVDCLookup.vdc_json[_ecsAuthentication.host]

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
                if type(local_zone_node_data[field]) is unicode:
                    try:
                        _logger.debug(MODULE_NAME + '::ecs_collect_data()::field from '
                                                    'local_zone_node_data being processed is: ' + field)
                        ecsdata[node_display_name][field] = float(local_zone_node_data[field])
                    except Exception as ex1:
                        try:
                            # We're here because trying to convert to a float failed.
                            ecsdata[node_display_name][field] = local_zone_node_data[field].encode("utf-8")
                        except Exception as ex2:
                            pass

                elif type(local_zone_node_data[field]) is list:
                    _logger.debug(MODULE_NAME + '::ecs_collect_data()::field from '
                                                'local_zone_node_data being processed is: ' + field)
                    ecsmgmntapi.get_ecs_detail_data(field=field, metric_list=local_zone_node_data[field],
                                                    metric_values=ecsdata_metrics[node_display_name])

                else:
                    _logger.debug(MODULE_NAME + '::ecs_collect_data()::field from '
                                                'local_zone_node_data being processed is: ' + field)
                    ecsmgmntapi.get_ecs_summary_data(field=field, summary_dict=local_zone_node_data[field],
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
            _influxClient.write_points(db_array)
            _logger.debug(MODULE_NAME + '::ecs_collect_data()::Local Zone Node data db_array is: \r\n\r\n'.join(str(db_array)))

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

            _influxClient.write_points(db_array)
        _logger.debug(MODULE_NAME + '::ecs_collect_data()::Local Zone Node metrics db_array is: \r\n\r\n'.join(str(db_array)))

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

            _influxClient.write_points(db_array)
            _logger.debug(MODULE_NAME + '::ecs_collect_data()::Local Zone Node summary db_array is: \r\n\r\n'.join(str(db_array)))


def ecs_authenticate():
    global _ecsAuthentication
    global _configuration
    global _logger

    try:
        # Wait till configuration is set
        while not _configuration:
            time.sleep(1)

        # Authenticate to ECS
        auth = ECSAuthentication(_configuration.protocol, _configuration.host, _configuration.user,
                                 _configuration.password, _configuration.port, _logger)
        auth.connect()

        # Check to see if we have a token returned
        if auth.token is None:
            _logger.error(MODULE_NAME + '::ecs_init()::Unable to authenticate to ECS as configured.  '
                         'Please validate and try again.')
        else:
            _ecsAuthentication = auth

    except Exception as e:
        _logger.error(MODULE_NAME + '::ecs_init()::Cannot initialize plugin. Cause: '
                      + str(e) + "\n" + traceback.format_exc())


def influx_init():
    global _influxClient
    global _configuration
    global _logger

    try:
        # Wait till configuration is set
        while not _configuration:
            time.sleep(1)

        # Instantiate utility object and check to see if our database exists
        db_utility = InfluxUtility(_configuration, _logger)
        database_found = db_utility.check_db_exists(_configuration.database_name)

        # If database is not found then connect with no database, create the database, and then switch to it
        if not database_found:
            influx_client = InfluxDBClient(_configuration.database_host, _configuration.database_port,
                                           _configuration.database_user, _configuration.database_password, None)
            influx_client.create_database(_configuration.database_name)
            influx_client.switch_database(_configuration.database_name)
        else:
            # Connect to influx with existing database
            influx_client = InfluxDBClient(_configuration.database_host, _configuration.database_port, _configuration.database_user, _configuration.database_password,_configuration.database_name)

        if influx_client is None:
            _logger.error(MODULE_NAME + '::influx_init()::Unable to connect to Influx as configured.  '
                                       'Please validate and try again.')
        else:
            _logger.info(MODULE_NAME + '::influx_init()::Successfully connected to Influx as configured.')
            _influxClient = influx_client

    except Exception as e:
        _logger.error(MODULE_NAME + '::influx_init()::Cannot initialize Influx connection. Cause: '
                      + str(e) + "\n" + traceback.format_exc())


"""
Main 
"""
if __name__ == "__main__":

    # Create object to support controlled shutdown
    controlledShutdown = ECSDataCollectionShutdown()

    # Dump out application path
    currentApplicationDirectory = os.getcwd()
    configFilePath = os.path.abspath(os.path.join(currentApplicationDirectory, "configuration", CONFIG_FILE))
    vdcLookupFilePath = os.path.abspath(os.path.join(currentApplicationDirectory, "configuration", VDC_LOOKUP_FILE))

    print(MODULE_NAME + "__main__::Current directory is : " + currentApplicationDirectory)
    print(MODULE_NAME + "__main__::Configuration file path is: " + configFilePath)

    # Initialize configuration and VDC Lookup
    ecs_config(configFilePath, vdcLookupFilePath)

    # Initialize connection to ECS
    ecs_authenticate()

    # Initialize database connection
    influx_init()

    # Setup up while loop to read data at set intervals
    while True:
        time.sleep(INTERVAL)
        ecs_collect_data()
        if controlledShutdown.kill_now:
            print(MODULE_NAME + "__main__::Controlled shutdown completed.")
            break

