#!/bin/bash

POD_NAME="mycenae${1}"
LOGS="/tmp/mycenae-logs/"
docker rm -f "${POD_NAME}"

if ! make -C "${GOPATH}/src/github.com/uol/mycenae/" build ; then
    exit 1
fi

scyllaIPs=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" scylla1 scylla2 scylla3 | sed 's/^.*$/"&"/' | tr '\n' ',' | sed 's/.$//')
memcachedIPs=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" memcached1 memcached2 memcached3 | sed 's/^.*$/"&:11211"/' | tr '\n' ',' | sed 's/.$//')
solrIP=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" solr1)

sed -i 's/nodes = \[[^]]*\]/nodes = \['$scyllaIPs'\]/' ../config.toml
sed -i 's/pool = \[[^]]*\]/pool = \['$memcachedIPs'\]/' ../config.toml
sed -i 's/http\:\/\/[^\:]*\:8983/http\:\/\/'$solrIP'\:8983/' ../config.toml

if [ ! -d "${LOGS}" ]; then
    mkdir ${LOGS}
fi

pod_arguments=(
	'-it'
    '--detach'
    '--name' "${POD_NAME}"
    '--volume' "${GOPATH}/src/github.com/uol/mycenae/mycenae:/tmp/mycenae"
    '--volume' "${GOPATH}/src/github.com/uol/mycenae/config.toml:/config.toml"
    '--volume' "${LOGS}:/${LOGS}"
    '--entrypoint' '/tmp/mycenae'
    '-p' '8080:8080'
)

dockerCmd="docker run ${pod_arguments[@]} ubuntu:xenial"
eval "$dockerCmd"
echo "$dockerCmd"

echo 'Mycenae OK'

docker logs "${POD_NAME}"
