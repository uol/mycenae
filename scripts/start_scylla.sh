#!/bin/bash

image="jenkins.macs.intranet:5000/mycenae/scylladb:v2.3.2"
pod_name="scylla$1"

pod_arguments=(
    '-d'
    '--name' "${pod_name}"
	'--hostname' "${pod_name}"
	"--net=timeseriesNetwork"
)

if [ $1 -gt 1 ]; then
	cmd="docker run ${pod_arguments[@]} ${image} --seeds=$(docker inspect --format='{{ .NetworkSettings.Networks.timeseriesNetwork.IPAddress }}' scylla1)"
else
	cmd="docker run ${pod_arguments[@]} ${image}"	
fi

eval "${cmd}"
echo "${cmd}"

echo "${pod_name} OK"
