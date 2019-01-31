#!/bin/bash

docker rm -f scylla1 scylla2 scylla3

checkScyllaUpNodes () {
    upnodes=$(docker exec -it scylla1 sh -c "nodetool status" | grep UN | wc -l)
    while [ "$upnodes" != "$1" ]
    do
        sleep 1
        upnodes=$(docker exec -it scylla1 sh -c "nodetool status" | grep UN | wc -l)
        echo -ne "Waiting nodes to sync: (${upnodes}/3)"\\r
    done
}

./start_scylla.sh 1
sleep 30
./start_scylla.sh 2
sleep 30
./start_scylla.sh 3
sleep 30

checkScyllaUpNodes 3

docker cp $GOPATH/src/github.com/uol/mycenae/docs/scylladb.cql scylla1:/tmp/
scyllaIP=$(docker inspect --format "{{ .NetworkSettings.Networks.timeseriesNetwork.IPAddress }}" scylla1)

echo "Running: cqlsh"
docker exec -it scylla1 sh -c "cqlsh --request-timeout=300 ${scyllaIP} -u cassandra -p cassandra < /tmp/scylladb.cql"
echo "Scylla Cluster OK"
