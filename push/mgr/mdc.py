import dill
import time

from push.mgr.qm import QueueManager
import sys

m = QueueManager(address=('', int(sys.argv[1])), authkey=b'password')
m.connect()

jq = m.get_job_queue()
rq = m.get_result_queue()

while rq.qsize() > 0:
    rq.get_sync()

jq.put_sync(('add', 5, 6))
jq.put_sync(('sub', 10, 2))
while rq.qsize() < 2:
    time.sleep(0.1)
rv = rq.get_sync()
print(f"result queue: {rv}")
rv = rq.get_sync()
print(f"result queue: {rv}")
jq.put_sync(('exit',))
jq.put_sync(('exit',))
