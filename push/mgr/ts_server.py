import asyncio
import sys
import threading
import time
from multiprocessing import process

import dill
import tornado.gen
import tornado.httpserver
import tornado.ioloop
import tornado.web
from pysyncobj import SyncObj
from pysyncobj.batteries import ReplList

from push.mgr.batteries import MyReplDict, ReplTimeseries, ReplHostManager
from push.mgr.code_util import load_src
from push.mgr.host_resources import HostResources, GPUResources
from push.mgr.qm import QueueManager
from push.mgr.task import DoTask

print("starting")


def get_cluster_info(hosts, so):
    all_nodes = [so.selfNode, *so.otherNodes]
    all_host_resources = hosts.lockData()
    if so.selfNode.id not in all_host_resources:
        return 0, 0, {}
    this_host_resources = all_host_resources[so.selfNode.id]
    all_nodes = [x for x in all_nodes if hosts.isOwned(x.id)]
    if so.selfNode not in all_nodes:
        return 0, 0, {}
    all_nodes = sorted(all_nodes, key=lambda x: x.id)
    all_nodes = [x for x in all_nodes if this_host_resources.is_compatible(all_host_resources[x.id])]
    return len(all_nodes), all_nodes.index(so.selfNode), this_host_resources


# def mk_process_cb(hosts):
def process_ts_updates(idx_data, keys, data):
    # start_t = time.perf_counter_ns()
    cluster_size, partition_id, host_capabilities = get_cluster_info(repl_hosts, sync_lock)
    if cluster_size == 0:
        return
    # print(f"strat time 1: {(time.perf_counter_ns() - start_t) / 100000}")
    print(f"post-processing: {idx_data} {keys} {cluster_size} {partition_id}")
    # print(f"hashes: {[(x.id, hash(x), hash(x) % cluster_size) for x in repl_strategies.rawData()]}")
    #    owned_strategies = [x for x in repl_strategies.rawData() if hash(x) % cluster_size == partition_id]
    capable_strategies = [x for x in repl_strategies.rawData() if host_resources.has_capacity(x.requirements)]
    # print(f"capable_strategies: {capable_strategies}")
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
    # return process_ts_updates


repl_hosts = ReplHostManager(autoUnlockTime=5)
repl_ts = ReplTimeseries(on_append=process_ts_updates)
repl_kvstore = MyReplDict()
repl_strategies = ReplList()

gpu_capabilities = sys.argv[1]
selfAddr = sys.argv[2]  # "localhost:10000"
my_port = int(selfAddr.split(":")[1])
partners = sys.argv[3:]  # ["localhost:10001", "localhost:10002"]
sync_lock = SyncObj(selfAddr, partners, consumers=[repl_kvstore, repl_ts, repl_strategies, repl_hosts])

host_resources = HostResources.create()
host_resources.gpu = GPUResources(count=1 if 'GPU' in gpu_capabilities else 0)

while not repl_hosts.tryAcquire(sync_lock.selfNode.id, data=host_resources, sync=True):
    time.sleep(0.1)


class DoRegister:
    def apply(self, name, src):
        src = load_src(repl_kvstore, src)
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
        src = load_src(repl_kvstore, src)
        return src(*args, **kwargs)


dl = DoLambda()


class DoRegistry:
    def apply(self):
        return list(QueueManager._registry.keys())


dreg = DoRegistry()
dotask = DoTask(repl_kvstore)

QueueManager.register('do_register', callable=lambda: dr)
QueueManager.register('apply_lambda', callable=lambda: dl)
QueueManager.register('get_registry', callable=lambda: dreg)
QueueManager.register('kvstore', callable=lambda: repl_kvstore)
QueueManager.register('tasks', callable=lambda: dotask)
QueueManager.register('ts', callable=lambda: repl_ts)
QueueManager.register('host_resources', callable=lambda: host_resources)
QueueManager.register('strategies', callable=lambda: repl_strategies)

# print(f"booting: ")
# print(f"status: {sync_lock.getStatus()}")
# print(f"self: {sync_lock.selfNode}")
# print(f"others: {sync_lock.otherNodes}")
mgr_port = (my_port % 1000) + 50000
# print(f"manager port: {mgr_port}")


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

webserver = tornado.httpserver.HTTPServer(make_app(repl_kvstore))
web_port = 1000 + int(sync_lock.selfNode.id.split(":")[1])
print(f"my web port: {web_port}")
webserver.listen(web_port)

# print(f"starting webserver")
# tornado.ioloop.IOLoop.current().start()
loop = asyncio.get_event_loop()
try:
    loop.run_forever()
finally:
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()
# print(f"stopping webserver")

mt.join()
