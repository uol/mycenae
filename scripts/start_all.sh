#!/bin/bash

NETWORK_NAME="timeseriesNetwork"

if [ "$1" == "reset" ]; then 
    echo "reseting $NETWORK_NAME network"
    docker rm -f $(docker ps -a -q)
    docker network rm $NETWORK_NAME
fi

network=$(docker network ls | grep "$NETWORK_NAME")
if [ -z "$network" ]; then
    docker network create -d bridge $NETWORK_NAME
fi

./start_scylla_cluster.sh
./start_solr_cluster.sh
./start_grafana.sh
./start_memcached_pool.sh
./start_mycenae.sh