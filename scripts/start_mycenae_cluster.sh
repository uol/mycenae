#!/bin/bash

checkMycenaeNodes () {
    upnodes=$(docker logs mycenae1 | grep 'mycenae started successfully')
    while [ -z "$upnodes" ]
    do
        sleep 1
        upnodes=$(docker logs mycenae1 | grep 'mycenae started successfully')
        echo -ne "Waiting initial setup..."\\r
    done
    echo "initial setup is done!"
}

docker rm -f mycenae1 mycenae2

if ! make -C "${GOPATH}/src/github.com/uol/mycenae/" build ; then
    exit 1
fi

scyllaIPs=$(docker inspect --format "{{ .NetworkSettings.Networks.timeseriesNetwork.IPAddress }}" scylla1 scylla2 scylla3 | sed 's/^.*$/"&"/' | tr '\n' ',' | sed 's/.$//')
memcachedIPs=$(docker inspect --format "{{ .NetworkSettings.Networks.timeseriesNetwork.IPAddress }}" memcached1 memcached2 memcached3 | sed 's/^.*$/"&:11211"/' | tr '\n' ',' | sed 's/.$//')
solrIP=$(docker inspect --format "{{ .NetworkSettings.Networks.timeseriesNetwork.IPAddress }}" solr1)

sed -i 's/nodes = \[[^]]*\]/nodes = \['$scyllaIPs'\]/' ../config.toml
sed -i 's/pool = \[[^]]*\]/pool = \['$memcachedIPs'\]/' ../config.toml
sed -i 's/http\:\/\/[^\:]*\:8983/http\:\/\/'$solrIP'\:8983/' ../config.toml

./start_mycenae.sh 1 2

checkMycenaeNodes

./start_mycenae.sh 2 1
