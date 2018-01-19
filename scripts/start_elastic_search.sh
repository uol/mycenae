#!/bin/bash

POD_NAME='elasticsearch'

docker rm -f "${POD_NAME}"

#docker run -d --name "${POD_NAME}" -v $GOPATH/src/github.com/uol/mycenae/docs/elasticsearch.yml:/etc/elasticsearch/elasticsearch.yml elasticsearch:2.4.1
docker run -p 9200:9200 -d --name "${POD_NAME}" jenkins.macs.intranet:5000/macs-elasticsearch-timeseries:v5

sleep 3

docker exec "${POD_NAME}" curl --silent -H "Content-Type: application/json" -X POST \
-d '{
	"mappings": {
		"meta": {
			"properties": {
				"tagsNested": {
					"type": "nested",
					"properties": {
						"tagKey": {
							"type": "string"
						},
						"tagValue": {
							"type": "string"
						}
					}
				}
			}
		},
		"metatext": {
			"properties": {
				"tagsNested": {
					"type": "nested",
					"properties": {
						"tagKey": {
							"type": "string"
						},
						"tagValue": {
							"type": "string"
						}
					}
				}
			}
		}
	}
}' http://127.0.0.1:9200/stats

echo "Elastic Search OK"
