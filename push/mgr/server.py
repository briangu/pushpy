import queue as Queue
import threading

import dill

from push.loader import compile_source
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


class DoRegister:
    def apply(self, name, src):
        # print(name)
        # print(name, src)
        # print(type(src))
        # src = src.decode('utf-8')
        src = dill.loads(src)
        q = src()
        # print(q)
        # exec(src)
        # print(type(src))
        # d = compile_source(src)
        QueueManager.register(name, callable=lambda: q)


dr = DoRegister()


class DoLambda:
    def apply(self, src, *args, **kwargs):
        src = dill.loads(src)
        q = src()
        return q.apply(*args, **kwargs)


dl = DoLambda()

QueueManager.register('get_job_queue', callable=lambda: job_queue)
QueueManager.register('get_result_queue', callable=lambda: result_queue)
QueueManager.register('do_add', callable=lambda: da)
QueueManager.register('do_register', callable=lambda: dr)
QueueManager.register('do_lambda', callable=lambda: dl)


# Start up
m = QueueManager(address=('', 50000), authkey=b'password')
s = m.get_server()
s.serve_forever()
