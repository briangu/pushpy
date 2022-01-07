#/bin/bash

$PUSH_HOME/push/examples/run_server.sh GPU 10000 10001 10002 > /tmp/push.10000.txt 2> /tmp/push.10000.err &
$PUSH_HOME/push/examples/run_server.sh CPU 10001 10000 10002 > /tmp/push.10001.txt 2> /tmp/push.10001.err &
$PUSH_HOME/push/examples/run_server.sh CPU 10002 10001 10000 > /tmp/push.10002.txt 2> /tmp/push.10002.err &

python3 $PUSH_HOME/push/push_repl.py 50000

pkill -f push_server
