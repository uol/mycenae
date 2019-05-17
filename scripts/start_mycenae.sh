#!/bin/bash

POD_NAME="mycenae${1}"
docker rm -f "${POD_NAME}"

LOGS="/tmp/mycenae-logs/"

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
    '--net=timeseriesNetwork'
    '--hostname' "${POD_NAME}"
    '--add-host' "loghost:182.168.0.25${1}"
    '--add-host' "mycenae${2}:182.168.0.25${2}"
    '--ip' "182.168.0.25${1}"
)

dockerCmd="docker run ${pod_arguments[@]} ubuntu:xenial"
eval "$dockerCmd"
echo "$dockerCmd"

echo 'Mycenae OK'
