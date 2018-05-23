#!/bin/bash

POD_NAME="solr${1}"

zookeeperIP=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" zookeeper)

docker rm -f "${POD_NAME}"

docker run -d --name "${POD_NAME}" -v $(pwd)/solr-configs:/solr-configs --restart always solr:7.3.1-alpine -cloud -z ${zookeeperIP}

sleep 2

echo "Solr OK"
