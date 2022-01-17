#!/bin/bash

cd /scm/push
pip install -e .

cd /tmp

echo $* 
eval $*
