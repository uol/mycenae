#!/bin/bash

docker rm -f scylla1 scylla2 scylla3

checkScyllaUpNodes () {
    upnodes=$(docker exec -it scylla1 sh -c "/opt/scylla-tools/bin/nodetool status" | grep UN | wc -l)
    while [ "$upnodes" != "$1" ]
    do
        sleep 1
        upnodes=$(docker exec -it scylla1 sh -c "/opt/scylla-tools/bin/nodetool status" | grep UN | wc -l)
        echo "Upnodes: $upnodes"
    done
}

./start_scylla.sh 1
sleep 10
./start_scylla.sh 2
sleep 10
./start_scylla.sh 3
sleep 10
checkScyllaUpNodes 3
sleep 30

docker cp $GOPATH/src/github.com/uol/mycenae/docs/scylladb.cql scylla1:/tmp/
scyllaIP=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" scylla1)
docker exec -it scylla1 sh -c "cqlsh ${scyllaIP} -u cassandra -p cassandra < /tmp/scylladb.cql"
consulServerIp=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" consulServer)

for i in {1..3}
do
	cmd="docker exec -d -it scylla${i} consul agent -server -node scylla${i} -join ${consulServerIp} -data-dir /tmp/consul"
	echo "${cmd}"
	eval "${cmd}"
	curl --silent -XPUT -d '{"name":"scylla","port":9042}' --header "Content-type: application/json" "http://${consulServerIp}:8500/v1/agent/service/register"
done

echo "Scylla Cluster OK"
