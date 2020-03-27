#!/bin/bash
go test -v -count=1 -timeout 120s github.com/uol/timeline/tests/http
go test -v -count=1 -timeout 120s github.com/uol/timeline/tests/opentsdb
