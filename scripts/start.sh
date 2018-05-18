#!/bin/bash

./start_scylla_cluster.sh
./start_zookeeper.sh
./start_solr.sh 1
./start_solr.sh 2
./start_solr.sh 3
./start_grafana.sh
./start_memcached_pool.sh
./start_mycenae.sh