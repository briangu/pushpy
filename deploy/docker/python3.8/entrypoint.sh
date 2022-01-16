#!/bin/bash

cd /scm/push
pip install -e .

cd /data

echo $* 
eval $*
