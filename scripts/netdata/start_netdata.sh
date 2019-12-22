#!/bin/bash

docker rm -f netdata

docker build -t uol-netdata:v1.0.0 .

mycenaeIP=$(docker inspect --format "{{ .NetworkSettings.Networks.timeseriesNetwork.IPAddress }}" mycenae1)

docker run -d --name=netdata \
            -p 19999:19999 \
            -v /proc:/host/proc:ro \
            -v /sys:/host/sys:ro \
            -v /var/run/docker.sock:/var/run/docker.sock:ro \
            --cap-add SYS_PTRACE \
            --security-opt apparmor=unconfined \
            --net=timeseriesNetwork \
            '--add-host' "mycenae:$mycenaeIP" \
            uol-netdata:v1.0.0

echo "netdata started!"