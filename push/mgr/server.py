import multiprocessing
import queue as Queue
import threading

import dill

from push.examples.d.lock import Lock
from push.mgr.qm import QueueManager
import psutil

from tensorflow.python.client import device_lib


def get_available_gpus():
    local_device_protos = device_lib.list_local_devices()
    return [x.name for x in local_device_protos if x.device_type == 'GPU']


# https://gist.github.com/spacecowboy/1203760

'''
Taken directly from the examples for multiprocessing. The only purpose for this
file is to serve two queues for clients, of which there are two. 
'''
selfAddr = "localhost:10000"
partners = []  # ["localhost:10001", "localhost:10002"]
sync_lock = Lock(selfAddr, partners, 10.0)

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
        stats = {}
        stats['cpu_count'] = multiprocessing.cpu_count()
        stats['virtual_memory'] = psutil.virtual_memory()
        stats['GPUs'] = get_available_gpus()
        return stats


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
m = QueueManager(address=('', 50000), authkey=b'password')
s = m.get_server()
s.serve_forever()
