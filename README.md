# couch-to-influx
Node libary to stream CouchDB changes into InfluxDB

This is a very basic but working (hacked version of couch-to-postgres) - currently only supports inserts - I am not really planing on extending it much further as this is a stepping stone project to give me some insight into how grafana's backends work.

The eventual plan is to add a backend to grafana which would query a postgres db populated by statsd backed by couchdb.

The couchdb database is expected to be in the format provided by https://github.com/sysadminmike/couch-statsd-backend.

The two projects give a basic version of the statsd influxdb backend but with couchdb in between so could be useful if you want to archive your metrics and then copy a specific range to influxdb.




