#!/bin/bash
go test -race -v -p 1 -count 1 -timeout 360s github.com/uol/timelinemanager/tests
