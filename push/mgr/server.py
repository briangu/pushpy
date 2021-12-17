import multiprocessing
import queue as Queue
import threading

import dill

from push.examples.d.lock import Lock
from push.mgr.qm import QueueManager
import psutil
import sys


# from tensorflow.python.client import device_lib


# TODO: GPU enabled systems can have a GPU client attach and listen to a queue to do work
#       we can report if there's a client registered or not
#       Question: how do we register lambdas w/ the GPU client?  send serialized?
#                 how do we sync the answer with the request? request id?
#       if we host the GPU processing in this server, then we may need a lock to guard the GPU
def get_available_gpus():
    return []
    # local_device_protos = device_lib.list_local_devices()
    # return [x.name for x in local_device_protos if x.device_type == 'GPU']


# https://gist.github.com/spacecowboy/1203760

'''
Taken directly from the examples for multiprocessing. The only purpose for this
file is to serve two queues for clients, of which there are two. 
'''

selfAddr = sys.argv[1]  # "localhost:10000"
partners = sys.argv[2:]  # ["localhost:10001", "localhost:10002"]
sync_lock = Lock(selfAddr, partners, 10.0)


class DoOnAcquire:
    def apply(selfself, p, c, t):
        print(p, c, t)

dacq = DoOnAcquire()

QueueManager.register("on_acquire", callable=lambda: dacq)

handle_map = dict()

def on_sync(method, path, clientId, t):
    global handle_map
    if method in handle_map:
        handle_map[method](path, clientId, t)


sync_lock.subscribe(on_sync)

# Define two queues, one for putting jobs on, one for putting results on.
job_queue = Queue.Queue()
result_queue = Queue.Queue()


class DoAdd:
    def apply(self, x, y):
        print(threading.current_thread().ident)
        return x + y


da = DoAdd()


class DoRegister:
    def apply(self, name, src):
        src = dill.loads(src)
        q = src()
        QueueManager.register(name, callable=lambda: q)


dr = DoRegister()


class DoRegisterCallback:
    def apply(self, name, src):
        global handle_map
        src = dill.loads(src)
        if isinstance(src, type):
            q = src()
            handle_map[name] = q.apply if hasattr(q, 'apply') else q
        else:
            handle_map[name] = src

drc = DoRegisterCallback()
QueueManager.register("do_register_callback", callable=lambda: drc)


class DoLambda:
    def apply(self, src, *args, **kwargs):
        src = dill.loads(src)
        q = src()
        return q.apply(*args, **kwargs)


dl = DoLambda()


class DoRegistry:
    def apply(self):
        return list(QueueManager._registry.keys())


dreg = DoRegistry()


class DoLocaleCapabilities:
    def apply(self):
        return {
            'cpu_count': multiprocessing.cpu_count(),
            'virtual_memory': psutil.virtual_memory(),
            'GPUs': get_available_gpus()
        }


dlc = DoLocaleCapabilities()

QueueManager.register('get_job_queue', callable=lambda: job_queue)
QueueManager.register('get_result_queue', callable=lambda: result_queue)
QueueManager.register('do_add', callable=lambda: da)

QueueManager.register('do_register', callable=lambda: dr)
QueueManager.register('apply_lambda', callable=lambda: dl)
QueueManager.register('get_registry', callable=lambda: dreg)
QueueManager.register('sync_lock', callable=lambda: sync_lock)
QueueManager.register('locale_capabilities', callable=lambda: dlc)

# Start up
mgr_port = (int(sys.argv[1].split(":")[1]) % 1000) + 50000
print(mgr_port)
m = QueueManager(address=('', mgr_port), authkey=b'password')
s = m.get_server()
# TODO: i think this code can be rewritten to use asyncio / twisted
s.serve_forever()
