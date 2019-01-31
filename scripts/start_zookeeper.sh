#!/bin/bash

POD_NAME='zookeeper'

docker rm -f "${POD_NAME}"

docker run -d --name "${POD_NAME}" --net=timeseriesNetwork zookeeper:3.4.11

sleep 2

echo "Zookeeper OK"
