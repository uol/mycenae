#!/bin/bash

./start_scylla_cluster.sh
./start_solr_cluster.sh
./start_grafana.sh
./start_memcached_pool.sh
./start_mycenae.sh