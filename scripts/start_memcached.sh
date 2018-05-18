#!/bin/bash

POD_NAME="memcached${1}"
docker rm -f "${POD_NAME}"

arguments=(
	'-d'
	'--name' "${POD_NAME}"
)

docker run "${arguments[@]}" 'jenkins.macs.intranet:5000/mycenae/memcached:v1'

echo "Memcached OK"