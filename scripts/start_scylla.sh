#!/bin/bash

image="jenkins.macs.intranet:5000/mycenae/scylladb:v1"
pod_name="scylla$1"

pod_arguments=(
    '-d'
    '--name' "${pod_name}"
)

if [ $1 -gt 1 ]; then
	cmd="docker run ${pod_arguments[@]} ${image} --seeds=$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' scylla1)"
else
	cmd="docker run ${pod_arguments[@]} ${image}"	
fi

eval "${cmd}"
echo "${cmd}"

echo "${pod_name} OK"
