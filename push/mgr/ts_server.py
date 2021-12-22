import multiprocessing
import sys

import dill
import numpy as np
import pandas as pd
import psutil
from pysyncobj import SyncObj, replicated, replicated_sync, SyncObjConsumer
from pysyncobj.batteries import ReplDict

from push.mgr.code_util import load_src
from push.mgr.qm import QueueManager
from push.mgr.task import DoTask

print("starting")

class MyReplDict(ReplDict):

    @replicated_sync
    def set_sync(self, key, value):
        self.set(key, value, _doApply=True)


kvstore = MyReplDict()


class ReplTimeseries(SyncObjConsumer):
    def __init__(self):
        super(ReplTimeseries, self).__init__()
        self.__data = dict()
        self.__index_data = list()

    @replicated
    def reset(self):
        self.__data = dict()
        self.__index_data = list()

    @replicated
    def append(self, idx_data, keys, data):
        self.__index_data.append(idx_data)
        for key, key_data in zip(keys, data):
            col = self.__data.get(key)
            if col is None:
                col = list()
                self.__data[key] = col
            key_data = key_data if isinstance(key_data, list) else [key_data]
            col.append(key_data)

    def flatten(self, keys=None):
        keys = keys or list(self.__data.keys())
        df = pd.DataFrame(columns=keys, index=self.__index_data)
        for key in keys:
            df[key] = np.concatenate(self.__data[key])
        return df


repl_ts = ReplTimeseries()

selfAddr = sys.argv[1]  # "localhost:10000"
partners = sys.argv[2:]  # ["localhost:10001", "localhost:10002"]
sync_lock = SyncObj(selfAddr, partners, consumers=[kvstore, repl_ts])

class DoRegister:
    def apply(self, name, src):
        src = dill.loads(src)
        q = src()
        QueueManager.register(name, callable=lambda: q)


dr = DoRegister()


class DoRegisterCallback:
    def apply(self, name, src):
        global onrep
        src = dill.loads(src)
        if isinstance(src, type):
            q = src()
            onrep.handle_map[name] = q.apply if hasattr(q, 'apply') else q
        else:
            onrep.handle_map[name] = src


drc = DoRegisterCallback()
QueueManager.register("do_register_callback", callable=lambda: drc)


class DoLambda:
    def apply(self, src, *args, **kwargs):
        src = load_src(kvstore, src)
        return src(*args, **kwargs)


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
            'GPUs': []
        }


class DoKvStore():
    def set(self, k, v):
        global kvstore
        # kvstore.set(k, v)
        print(k, v)

    def get(self, k):
        global kvstore
        # return kvstore.get(k)
        print(k)


dkvs = DoKvStore()

dotask = DoTask()

dlc = DoLocaleCapabilities()

print(dlc.apply())

QueueManager.register('do_register', callable=lambda: dr)
QueueManager.register('apply_lambda', callable=lambda: dl)
QueueManager.register('get_registry', callable=lambda: dreg)
QueueManager.register('kvstore', callable=lambda: kvstore)
QueueManager.register('tasks', callable=lambda: dotask)
QueueManager.register('ts', callable=lambda: repl_ts)
QueueManager.register('locale_capabilities', callable=lambda: dlc)

print(f"booting")

# Start up
mgr_port = (int(sys.argv[1].split(":")[1]) % 1000) + 50000
print(mgr_port)
m = QueueManager(address=('', mgr_port), authkey=b'password')
s = m.get_server()
# TODO: i think this code can be rewritten to use asyncio / twisted
s.serve_forever()
