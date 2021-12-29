import asyncio
import sys
import time

from pysyncobj import SyncObj

from push.mgr.batteries import ReplHostManager
from push.mgr.host_resources import HostResources, GPUResources, get_cluster_info
from push.mgr.qm import QueueManager
from push.mgr.qm_util import serve_forever
from push.mgr.ts_boot import create_subconsumers

gpu_capabilities = sys.argv[1]
selfAddr = sys.argv[2]
base_port = int(selfAddr.split(":")[1])
partners = sys.argv[3:]

# fake GPU for testing
host_resources = HostResources.create()
host_resources.gpu = GPUResources(count=1 if 'GPU' in gpu_capabilities else 0)


class DoRegistry:
    def apply(self):
        return list(QueueManager._registry.keys())


QueueManager.register('get_registry', callable=lambda: DoRegistry())
QueueManager.register('host_resources', callable=lambda: host_resources)

# >>> setup sync obj
repl_hosts = ReplHostManager(autoUnlockTime=5)
boot_consumers, repl_globals, qm_methods = create_subconsumers(base_port)
flat_consumers = [repl_hosts, *boot_consumers]
sync_obj = SyncObj(selfAddr, partners, consumers=flat_consumers)

QueueManager.register('sync_obj', callable=lambda: sync_obj)

globals()['get_cluster_info'] = get_cluster_info

for k, v in repl_globals.items():
    globals()[k] = v

for k, v in qm_methods.items():
    QueueManager.register(k, v)

while not repl_hosts.tryAcquire(sync_obj.selfNode.id, data=host_resources, sync=True):
    time.sleep(0.1)


# <<< setup sync obj


# TODO: i think this code can be rewritten to use asyncio / twisted
mgr_port = (base_port % 1000) + 50000
m, mt = serve_forever(mgr_port, b'password')

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
