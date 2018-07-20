# ecs-pulse
----------------------------------------------------------------------------------------------
logstash.conf info

This configuration will listen for ECS Data Access log entries in SYSLOG format coming in on port 5000 for both TCP and UDP.  

It will then parse them using the GROK filter which will strip out the individual field elements.  

Finally, it will add them to Elasticsearch in daily indices prefixed with "ecs-access-index-".

Note the 172.17.0.x subnet in the host setting for the ElasticSearch output plugin.  This is the subnet for the Docker network. Set what's appropriate for your environment.


