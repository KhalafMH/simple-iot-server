CREATE KEYSPACE sensor_app WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'docker_1': 1};

USE sensor_app;

CREATE TABLE readings (
    id text,
    year_month text,
    timestamp timestamp,
    type text,
    value double,
    alert boolean,
    PRIMARY KEY ((id, year_month), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
