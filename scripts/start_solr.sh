#!/bin/bash

POD_NAME="solr${1}"

zookeeperIP=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" zookeeper)

docker rm -f "${POD_NAME}"

docker run -d --name "${POD_NAME}" -e ZK_IP="${zookeeperIP}" -v $(pwd)/solr-configs:/opt/solr/server/solr/configsets/_default/conf --restart always jenkins.macs.intranet:5000/solr:v7.3.0

sleep 2

echo "Solr OK"
