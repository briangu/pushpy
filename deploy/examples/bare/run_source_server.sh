#!/bin/bash
# serve code via http
cd $PUSH_HOME
python3 -m http.server
