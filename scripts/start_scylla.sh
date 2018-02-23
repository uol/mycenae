#!/bin/bash

pod_name="scylla$1"

pod_arguments=(
    '-d'
    '-it'
    '--name' "${pod_name}"
    '--privileged'
    '-ti'
     '-e' "container=docker"
     '-v' '/sys/fs/cgroup:/sys/fs/cgroup'
)

cmd="docker run ${pod_arguments[@]} jenkins.macs.intranet:5000/mycenae/scylla-centos:v2 /usr/sbin/init"
eval "${cmd}"
echo "${cmd}"

seedIP='127.0.0.1'

if [ $1 -gt 1 ]
    then
        seedIP="$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' scylla1)"
fi

localhostIp=$(ifconfig | grep -A1 docker0 | grep 'inet addr' | awk '{print $2}' | awk -F':' '{print $2}')
cmd="echo ${localhostIp} docker_localhost >> /etc/hosts"
docker exec ${pod_name} /bin/sh -c "${cmd}"

cmd="docker exec ${pod_name} setup.sh ${seedIP} dc_gt_a1 1al"
echo "${cmd}"
eval "${cmd}"

cmd="docker exec ${pod_name} systemctl start scylla-server"
echo "${cmd}"
eval "${cmd}"

echo "${pod_name} OK"
