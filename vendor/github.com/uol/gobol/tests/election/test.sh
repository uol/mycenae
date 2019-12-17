#!/bin/bash

# 
# Tests the cluster election process.
# author: rnojiri
#

function killAll {
    docker rm -f et1 et2 et3 zookeeper
}

function quit {
    killAll
    exit
}

function createSlave {
    docker run -it -d -h ${1} --name ${1} --add-host="zookeeper.intranet:${zkIP}" electiontest
    sleep 10
    grepResult=$(docker logs ${1} | grep 'slave node created')
    if [ -z "$grepResult" ]; then
        echo "FAIL: expecting ${1} be a slave"
        quit
    else 
        echo "OK: ${1} is slave"
    fi
}

killAll

# build the node image
export CGO_ENABLED=0
if go build; then
    echo "build OK!"
else
    echo "build FAILED!"
    exit
fi

docker build -t electiontest . 

# zookeeper
zkPodName='zookeeper'
docker rm -f "${zkPodName}"
docker run -d --name "${zkPodName}" zookeeper:3.4.11
sleep 5
zkIP=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" $zkPodName)
echo "$zkPodName listening on ip $zkIP"
echo "Zookeeper OK"
# /zookeeper

docker run -it -d -h et1 --name et1 --add-host="zookeeper.intranet:${zkIP}" electiontest
sleep 10
docker logs et1
grepResult=$(docker logs et1 | grep 'master node created')
if [ -z "$grepResult" ]; then
    echo "FAIL: expecting et1 be the master"
    quit
else 
    echo "OK: et1 is the master"
fi

createSlave et2
createSlave et3

sleep 10

grepResultMaster=$(docker logs et1 | grep 'cluster changed signal received' | wc -l)

if [ "$grepResultMaster" != "2" ]; then
    echo "FAIL: two 'cluster changed signal received' messages"
    quit
else
    echo "OK: master detected two cluster configuration changes"
fi

echo "killing master node..."
docker rm -f et1
sleep 10

grepResultET2=$(docker logs et2 | grep 'trying to be the new master')
grepResultET3=$(docker logs et3 | grep 'trying to be the new master')
if [ -z "$grepResultET2" ] && [ -z "$grepResultET3" ]; then
    echo "FAIL: expecting et2 and et3 to try to be the new master"
    quit
else
    echo "OK: et2 and et3 tried to be the new master"
fi

grepResultET2=$(docker logs et2 | grep 'master node created')
grepResultET3=$(docker logs et3 | grep 'master node created')
if [ -z "$grepResultET2" ] && [ -z "$grepResultET3" ]; then
    echo "FAIL: expecting et2 or et3 be the master"
    quit
else
    echo "OK: et2 or et3 is the master: ${grepResultET2}${grepResultET3}"
fi

createSlave 'et1'

echo "OK: test is done!"

quit