#!/bin/bash
CGO_ENABLED=0 go build
docker build -t electiontest . 
