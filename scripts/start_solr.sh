#!/bin/bash

POD_NAME="solr${1}"

zookeeperIP=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" zookeeper)

docker rm -f "${POD_NAME}"

docker run -d --name "${POD_NAME}" -e ZK_IP="${zookeeperIP}" --restart always jenkins.macs.intranet:5000/solr:v7.2.0

sleep 2

echo "Solr OK"
