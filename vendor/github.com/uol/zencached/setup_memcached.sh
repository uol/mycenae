#!/bin/bash

##
# Runs a docker using memcached image.
# author: rnojiri
##

INSTANCE="zencachedMemcached"
NETWORK_NAME="testNetwork"

network=$(docker network ls | grep "$NETWORK_NAME")
if [ -z "$network" ]; then
    docker network create --subnet 162.168.0.0/16 -d bridge $NETWORK_NAME
fi

memcachedIPs=()

for i in {1..3}; do

    memcachedID=$(docker ps -a -q --filter "name=${INSTANCE}${i}")

    if [ ! -z "$memcachedID" ]; then
        docker rm -f ${INSTANCE}${i}
    fi

    docker run --net=${NETWORK_NAME} --name ${INSTANCE}${i} -d memcached

    memcachedIP=`docker inspect --format="{{ .NetworkSettings.Networks.${NETWORK_NAME}.IPAddress }}" ${INSTANCE}${i}`

    memcachedIPs+=("${memcachedIP}")
done

printf "%s,%s,%s" "${memcachedIPs[0]}" "${memcachedIPs[1]}" "${memcachedIPs[2]}" 
