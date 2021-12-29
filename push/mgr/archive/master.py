import random

from mathenate import mathenate
from push.mgr.qm import QueueManager

import time


QueueManager.register('get_job_queue')
QueueManager.register('get_result_queue')

# Connect to server
m = QueueManager(address=('', 50000), authkey=b'password')
m.connect()

# Set up queus
job_queue = m.get_job_queue()
result_queue = m.get_result_queue()

time.sleep(random.randint(1, 10))

# Since we can't be sure that the slaves have  time to fill the result queue
# we clear the contents of the queue first
while not result_queue.empty():
    print("waiting results: " + str(result_queue.get()))

print('sending add-job')
job_queue.put(('add', 2, 3))

print('sending sub-job')
job_queue.put(('sub', 5, 1))

print('sending special-job')
job_queue.put((mathenate, 2, 10))

while not result_queue.empty():
    print("result = " + str(result_queue.get()))
