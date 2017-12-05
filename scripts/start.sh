#!/bin/bash

./start_consul_server.sh
./start_scylla_cluster.sh
./start_elastic_search.sh
./start_grafana.sh
./start_memcached_pool.sh
./start_mycenae.sh