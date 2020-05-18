#!/bin/bash

go test -v -count 1 ./tests/opentsdb/opentsdb_test.go
go test -v -count 1 ./tests/json/json_test.go