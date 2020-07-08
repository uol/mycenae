#!/bin/bash
go test -v -p 1 -count 1 -timeout 360s github.com/uol/timeline/tests/buffer
go test -v -p 1 -count 1 -timeout 360s github.com/uol/timeline/tests/opentsdb
go test -v -p 1 -count 1 -timeout 360s github.com/uol/timeline/tests/http
go test -v -p 1 -count 1 -timeout 360s github.com/uol/timeline/tests/config
go test -v -p 1 -count 1 -timeout 360s github.com/uol/timeline/tests/udp