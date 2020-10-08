#!/bin/bash
go test -race -v -p 1 -count 1 -timeout 360s github.com/uol/timeline/tests/buffer
go test -race -v -p 1 -count 1 -timeout 360s github.com/uol/timeline/tests/opentsdb
go test -race -v -p 1 -count 1 -timeout 360s github.com/uol/timeline/tests/http
go test -race -v -p 1 -count 1 -timeout 360s github.com/uol/timeline/tests/config
go test -race -v -p 1 -count 1 -timeout 360s github.com/uol/timeline/tests/udp