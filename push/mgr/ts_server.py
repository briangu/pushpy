import asyncio
import sys
import threading
import time
from multiprocessing import process

import dill
import numpy as np
import pandas as pd
import tornado.gen
import tornado.httpserver
import tornado.ioloop
import tornado.web
from pysyncobj import SyncObj, replicated, replicated_sync, SyncObjConsumer
from pysyncobj.batteries import ReplDict, ReplList

from push.mgr.code_util import load_src
from push.mgr.host_resources import HostResources, GPUResources
from push.mgr.qm import QueueManager
from push.mgr.repl_resource_leader import ReplHostManager
from push.mgr.task import DoTask

print("starting")

# TODO: add host capabilities to a data structure to capture the sub-partition sizes
#       use either the current strategy, which is the connectedness or
#         a repl lock so taht if the host disappears, we ignore it

# def in_same_cluster(hc1, hc2):
#     return not set(hc1['gpu_info']).isdisjoint(set(hc2['gpu_info']))


def get_cluster_info(so):
    all_nodes = [so.selfNode, *so.otherNodes]
    all_host_resources = repl_hosts.lockData()
    if so.selfNode.id not in all_host_resources:
        return 0, 0, {}
    this_host_resources = all_host_resources[so.selfNode.id]
    print(f"this host resources: {type(this_host_resources)} {this_host_resources}")
    all_nodes = [x for x in all_nodes if repl_hosts.isOwned(x.id)]
    if so.selfNode not in all_nodes:
        return 0, 0, {}
    all_nodes = sorted(all_nodes, key=lambda x: x.id)
    all_nodes = [x for x in all_nodes if this_host_resources.is_compatible(all_host_resources[x.id])]
    # print(all_nodes)
    return len(all_nodes), all_nodes.index(so.selfNode), all_host_resources[so.selfNode.id]


class MyReplDict(ReplDict):

    def __init__(self, on_set=None):
        super(MyReplDict, self).__init__()
        self.on_set = on_set

    @replicated_sync
    def set_sync(self, key, value):
        self.set(key, value, _doApply=True)

    @replicated
    def set(self, key, value):
        super().set(key, value, _doApply=True)
        if self.on_set is not None:
            self.on_set(key, value)


kvstore = MyReplDict()

repl_hosts = ReplHostManager(autoUnlockTime=5)


class ReplTimeseries(SyncObjConsumer):
    def __init__(self, on_append=None):
        super(ReplTimeseries, self).__init__()
        self.__data = dict()
        self.__index_data = list()
        self.__on_append = on_append

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
        if self.__on_append is not None:
            self.__on_append(idx_data, keys, data)

    def flatten(self, keys=None):
        keys = keys or list(self.__data.keys())
        df = pd.DataFrame(columns=keys, index=self.__index_data)
        for key in keys:
            df[key] = np.concatenate(self.__data[key])
        return df


def process_ts_updates(idx_data, keys, data):
    # start_t = time.perf_counter_ns()
    cluster_size, partition_id, host_capabilities = get_cluster_info(sync_lock)
    if cluster_size == 0:
        return
    # print(f"strat time 1: {(time.perf_counter_ns() - start_t) / 100000}")
    print(f"post-processing: {idx_data} {keys} {cluster_size} {partition_id}")
    # print(f"hashes: {[(x.id, hash(x), hash(x) % cluster_size) for x in repl_strategies.rawData()]}")
#    owned_strategies = [x for x in repl_strategies.rawData() if hash(x) % cluster_size == partition_id]
    capable_strategies = [x for x in repl_strategies.rawData() if host_resources.has_capacity(x.requirements)]
    print(f"capable_strategies: {capable_strategies}")
    owned_strategies = [x for x in capable_strategies if hash(x) % cluster_size == partition_id]
    print(f"owned strategies: {[x.id for x in owned_strategies]}")
    # print(f"owned strategies: {owned_strategies}")
    # print(f"strat time 2: {(time.perf_counter_ns() - start_t) / 100000}")
    sym_map = dict()
    for s in owned_strategies:
        # if not can_execute_strategy(host_capabilities, s):
        #     print(f"skipping unsupported strategy: {s.id} {s.capabilities}")
        #     continue
        for symbol in s.symbols:
            x = sym_map.get(symbol)
            if x is None:
                x = set()
                sym_map[symbol] = x
            x.add(s)
    # print(f"strat time: {(time.perf_counter_ns() - start_t) / 100000}")
    for symbol in keys:
        # print(f"symbol: {symbol}")
        strategies = sym_map.get(symbol)
        # if strategies is not None:
        #     print(f"applying strategies: {[x.id for x in strategies]}")


repl_ts = ReplTimeseries(on_append=process_ts_updates)

repl_strategies = ReplList()

gpu_capabilities = sys.argv[1]
selfAddr = sys.argv[2]  # "localhost:10000"
my_port = int(selfAddr.split(":")[1])
partners = sys.argv[3:]  # ["localhost:10001", "localhost:10002"]
sync_lock = SyncObj(selfAddr, partners, consumers=[kvstore, repl_ts, repl_strategies, repl_hosts])

host_resources = HostResources.create()
host_resources.gpu = GPUResources(count=np.random.randint(0, 2))

while not repl_hosts.tryAcquire(sync_lock.selfNode.id, data=host_resources, sync=True):
    time.sleep(0.1)


class DoRegister:
    def apply(self, name, src):
        src = dill.loads(src)
        q = src()
        QueueManager.register(name, callable=lambda: q)


dr = DoRegister()


# class DoRegisterCallback:
#     def apply(self, name, src):
#         global onrep
#         src = dill.loads(src)
#         if isinstance(src, type):
#             q = src()
#             onrep.handle_map[name] = q.apply if hasattr(q, 'apply') else q
#         else:
#             onrep.handle_map[name] = src
#
#
# drc = DoRegisterCallback()
# QueueManager.register("do_register_callback", callable=lambda: drc)


class DoLambda:
    def apply(self, src, *args, **kwargs):
        src = load_src(kvstore, src)
        return src(*args, **kwargs)


dl = DoLambda()


class DoRegistry:
    def apply(self):
        return list(QueueManager._registry.keys())


dreg = DoRegistry()

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

dotask = DoTask(kvstore)

# print(dlc.apply())

QueueManager.register('do_register', callable=lambda: dr)
QueueManager.register('apply_lambda', callable=lambda: dl)
QueueManager.register('get_registry', callable=lambda: dreg)
QueueManager.register('kvstore', callable=lambda: kvstore)
QueueManager.register('tasks', callable=lambda: dotask)
QueueManager.register('ts', callable=lambda: repl_ts)
# QueueManager.register('locale_capabilities', callable=lambda: capabilities)
QueueManager.register('strategies', callable=lambda: repl_strategies)

print(f"booting: ")
print(f"status: {sync_lock.getStatus()}")
print(f"self: {sync_lock.selfNode}")
print(f"others: {sync_lock.otherNodes}")
# cluster_size, my_partition = get_cluster_info(sync_lock)
# print(f"my_partition={my_partition}")

# Start up
mgr_port = (my_port % 1000) + 50000
print(f"manager port: {mgr_port}")


def serve_forever(mgr_port, auth_key):
    m = QueueManager(address=('', mgr_port), authkey=auth_key)
    mgmt_server = m.get_server()
    mgmt_server.stop_event = threading.Event()
    process.current_process()._manager_server = mgmt_server
    try:
        accepter = threading.Thread(target=mgmt_server.accepter)
        accepter.daemon = True
        accepter.start()
        return m, accepter
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(e)


def on_get(self):
    self.write("hello")
    self.write(f"keys: {self.kvstore.keys()}")
    self.finish()


class MainHandler(tornado.web.RequestHandler):

    def initialize(self, kvstore):
        if kvstore is None:
            m = QueueManager(address=('', 50000), authkey=b'password')
            m.connect()
            self.kvstore = m.kvstore()
        else:
            self.kvstore = kvstore
        if "on_get_v" not in self.kvstore:
            self.kvstore.set("on_get_v", 1)
            self.kvstore.set("on_get_c", dill.dumps(on_get))

    @tornado.gen.coroutine
    def get(self):
        on_get_v = self.kvstore.get("on_get_v")
        if on_get_v is not None:
            kv_on_get = self.kvstore.get(f"on_get_v{on_get_v}")
            if kv_on_get is not None:
                kv_on_get = load_src(self.kvstore, kv_on_get)
                kv_on_get(self)


def make_app(kvstore):
    return tornado.web.Application([
        ("/", MainHandler, {'kvstore': kvstore})
    ])


# TODO: i think this code can be rewritten to use asyncio / twisted
m, mt = serve_forever(mgr_port, b'password')

webserver = tornado.httpserver.HTTPServer(make_app(kvstore))
# port = 11000 + my_partition
port = 1000 + int(sync_lock.selfNode.id.split(":")[1])
print(f"my port: {port}")
webserver.listen(port)
# webserver.start(2)

print(f"starting webserver")
# tornado.ioloop.IOLoop.current().start()
loop = asyncio.get_event_loop()
try:
    loop.run_forever()
finally:
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()
print(f"stopping webserver")

mt.join()
