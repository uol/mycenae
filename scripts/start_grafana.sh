#!/bin/bash

POD_NAME='grafana'
docker rm -f "${POD_NAME}"

docker run -d --name "${POD_NAME}" --net=timeseriesNetwork -p 3000:3000  grafana/grafana:latest

curl --silent -POST -H "Content-Type: application/json" -u admin:admin -d '{"name": "stats","type": "opentsdb","access": "proxy","url": "http://localhost:8787/keyspaces/stats","basicAuth": false}' http://localhost:3000/api/datasources
curl --silent -POST -H "Content-Type: application/json" -u admin:admin -d @../docs/mycenae_dashboard http://localhost:3000/api/dashboards/db

echo "Grafana OK"
