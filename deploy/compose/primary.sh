#!/bin/bash
export PUSH_NODE_TYPE=primary
docker-compose -p "push" -f push-node-primary.yml $*
