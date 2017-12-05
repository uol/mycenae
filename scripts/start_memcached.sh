#!/bin/bash

POD_NAME="memcached${1}"
docker rm -f "${POD_NAME}"

arguments=(
	'-d'
	'--name' "${POD_NAME}"
)

docker run "${arguments[@]}" 'jenkins.macs.intranet:5000/mycenae/memcached:v1'

consulServerIp=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" consulServer)

cmd="docker exec -d -it ${POD_NAME} consul agent -server -node ${POD_NAME} -join ${consulServerIp} -data-dir /tmp/consul"
echo "${cmd}"
eval "${cmd}"

curl --silent -XPUT -d '{"name":"memcached","port":11211}' --header "Content-type: application/json" "http://${consulServerIp}:8500/v1/agent/service/register"

echo "Memcached OK"