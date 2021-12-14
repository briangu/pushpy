import queue as Queue
import threading

from push.mgr.qm import QueueManager

# https://gist.github.com/spacecowboy/1203760

'''
Taken directly from the examples for multiprocessing. The only purpose for this
file is to serve two queues for clients, of which there are two. 
'''

# Define two queues, one for putting jobs on, one for putting results on.
job_queue = Queue.Queue()
result_queue = Queue.Queue()


class DoAdd:
    def apply(self, x, y):
        print(threading.current_thread().ident)
        return x+y

da = DoAdd()


QueueManager.register('get_job_queue', callable=lambda: job_queue)
QueueManager.register('get_result_queue', callable=lambda: result_queue)
QueueManager.register('do_add', callable=lambda: da)

# Start up
m = QueueManager(address=('', 50000), authkey=b'password')
s = m.get_server()
s.serve_forever()
