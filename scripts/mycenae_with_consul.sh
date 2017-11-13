#!/bin/bash

CONSUL_POD_NAME="consulMycenae${1}"
POD_NAME="mycenae${1}"

#if ! make -C "${GOPATH}/src/${PACKAGE}" build ; then
#    exit 1
#fi

docker rm -f "${CONSUL_POD_NAME}"
docker rm -f "${POD_NAME}"

arguments=(
    '--detach'
    '--hostname' "${CONSUL_POD_NAME}"
    '--name' "${CONSUL_POD_NAME}"
    '--publish' '8787:8080'
)

CONSUL_HOST=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" consulServer)

consul_arguments=(
    '--join' "${CONSUL_HOST}"
    '--retry-join' "${CONSUL_HOST}"
    '-recursor' "192.168.206.8"
)

docker run "${arguments[@]}" "progrium/consul" "${consul_arguments[@]}"

SCYLLA_HOST=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" consulScylla1)
ELASTIC_HOST=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" elastic)

pod_arguments=(
    '--detach'
    '--name' "${POD_NAME}"
    '--network' "container:${CONSUL_POD_NAME}"
    '--volume' "${GOPATH}/src/github.com/uol/mycenae/mycenae:/tmp/mycenae"
    '--volume' "${GOPATH}/src/github.com/uol/mycenae/config-scylla.toml:/config.toml"
    '--entrypoint' '/tmp/mycenae'
)

docker run "${pod_arguments[@]}" "ubuntu:xenial"

sleep 5

curl --silent -XPUT --header "Content-type: application/json" "http://localhost:8500/v1/agent/service/register" \
-d '{
        "name":"mycenae1",
        "port":8080
}'