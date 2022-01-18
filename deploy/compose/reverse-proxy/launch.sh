#!/bin/bash
product=`basename $1 .yml`
docker-compose -p "$product" -f push-cluster.yml $*
