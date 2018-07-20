# ecs-pulse configuration
----------------------------------------------------------------------------------------------
ecs-pulse is a PYTHON based data collection module for DELL EMC's Elastic Cloud Storage Product

We've provided two sample configuration files:

- ecs_pulse_config.sample: Change file suffix from .sample to .json and configure as needed
  This contains the tool configuration for ECS and database connetion, logging level, etc. Here
  is the sample configuration:
  
  BASE:
  logging_level - The default is "info" but it can be set to "debug" to generate a LOT of details
  datastore - This is a placeholder for future datastores.  At the moment it's set to "influx"
  
  ECS_CONNECTION:
  protocol - Should be set to "https"
  host - This is the IP address of FQDN of an ECS node
  port - This is always "4443" which is the ECS Management API port
  user - This is the user id of an ECS Management User 
  password - This is the password for the ECS Management User
  
  INFLUX_DATABASE_CONNECTION:
  host = This is the IP address of FQDN of the InfluxDB server
  port - This is the port that the InfluxDB server is listening on.  Default is "8086"
  user - This is the user id of the InfluxDB user 
  password - This is the password of the InfluxDB user 
  databasename - The name of the InfluxDB to connect to
  
- ecs_vdc_lookup.sample: Change file suffix from .sample to .json and configure as needed
  This contains a manual map of ip addresses to ECS VDC name.  This is a temporary setup workaround till we 
  dynamically grab the name during data collection.  This is simply a JSON dictionary of IP addresses to 
  VDC names.
  
  {
  "xx.xx.xx.xx": "ECSCSE-test-vdc1",
  "xx.xx.xx.xx": "ECSCSE-test-vdc1",
  "xx.xx.xx.xx": "ECSCSE-test-vdc1",
  "xx.xx.xx.xx": "ECSCSE-test-vdc1",
  "xx.xx.xx.xx": "ECSCSE-test-vdc1",
  "xx.xx.xx.xx": "ECSCSE-test-vdc2",
  "xx.xx.xx.xx": "ECSCSE-test-vdc2",
  "xx.xx.xx.xx": "ECSCSE-test-vdc2",
  "xx.xx.xx.xx": "ECSCSE-test-vdc2",
  "xx.xx.xx.xx": "ECSCSE-test-vdc2"
}

