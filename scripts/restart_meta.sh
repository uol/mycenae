#!/bin/bash

docker rm -f solr1 solr2 solr3 zookeeper mycenae memcached1 memcached2 memcached3
./start_zookeeper.sh
./start_solr.sh 1
./start_solr.sh 2
./start_solr.sh 3
./start_memcached_pool.sh
./start_mycenae.sh