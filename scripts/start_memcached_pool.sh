#!/bin/bash

./start_memcached.sh 1
./start_memcached.sh 2
./start_memcached.sh 3

echo 'Memcached Pool OK'