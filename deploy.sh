#!/bin/bash
set -e
./build.sh
docker-compose -f docker-compose.yml -f production.yml up --detach