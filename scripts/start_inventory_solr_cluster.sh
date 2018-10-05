#!/bin/bash

docker rm -f solr1 solr2 solr3 zookeeper
./start_zookeeper.sh
./start_solr.sh 1

zookeeperIp=`docker inspect --format='{{ .NetworkSettings.IPAddress }}' zookeeper`
docker exec -it solr1 /opt/solr/bin/solr zk cp file:/solr-configs/solr.xml zk:/solr.xml -z ${zookeeperIp}:2181
docker exec -it solr1 /opt/solr/bin/solr zk upconfig -n inventory -d /solr-configs/inventory -z ${zookeeperIp}:2181

./start_solr.sh 2
./start_solr.sh 3
