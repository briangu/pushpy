#/bin/bash

$PUSH_HOME/deploy/bare/run_server.sh GPU localhost:10000 localhost:10001 localhost:10002 > /tmp/push.10000.txt 2> /tmp/push.10000.err &
$PUSH_HOME/deploy/bare/run_server.sh CPU localhost:10001 localhost:10000 localhost:10002 > /tmp/push.10001.txt 2> /tmp/push.10001.err &
$PUSH_HOME/deploy/bare/run_server.sh CPU localhost:10002 localhost:10001 localhost:10000 > /tmp/push.10002.txt 2> /tmp/push.10002.err &

python3 $PUSH_HOME/pushpy/push_repl.py 50000

pkill -f push_server
