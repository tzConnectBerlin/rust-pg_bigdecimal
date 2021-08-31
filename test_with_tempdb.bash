#!/bin/bash

d=`
docker run -d \
    -p 15432:5432 \
    -e POSTGRES_PASSWORD=test \
    -e POSTGRES_USER=test \
    -e POSTGRES_DB=test \
    postgres`
[ $? -ne 0 ] && exit 1;

DB_SETUP=3s
echo "waiting for db setup for $DB_SETUP.."
sleep $DB_SETUP

cargo test

docker kill "$d" >/dev/null
