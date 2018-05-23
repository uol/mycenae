#!/bin/bash

./start_solr_cluster.sh
docker rm -f mycenae memcached1 memcached2 memcached3
./start_memcached_pool.sh
./start_mycenae.sh