#!/bin/bash

POD_NAME="solr${1}"

zookeeperIP=$(docker inspect --format "{{ .NetworkSettings.Networks.timeseriesNetwork.IPAddress }}" zookeeper)

docker rm -f "${POD_NAME}"

docker run -d --name "${POD_NAME}" "--net=timeseriesNetwork" -v $(pwd)/solr-configs:/solr-configs --restart always solr:7.4.0-alpine -cloud -z ${zookeeperIP}

sleep 2

echo "Solr OK"
