import asyncio
import sys
import time

import tornado.httpserver
import tornado.web
from pysyncobj import SyncObj, SyncObjConsumer

from push.mgr.batteries import ReplLockDataManager
from push.mgr.host_resources import HostResources, GPUResources, get_cluster_info
from push.mgr.push_manager import PushManager
from push.mgr.push_util import serve_forever
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
        return list(PushManager._registry.keys())


PushManager.register('get_registry', callable=lambda: DoRegistry())
PushManager.register('host_resources', callable=lambda: host_resources)

# >>> setup sync obj
repl_hosts = ReplLockDataManager(autoUnlockTime=5)
m_globals, web_app = create_subconsumers(base_port)
boot_consumers = [x for x in m_globals.values() if isinstance(x, SyncObjConsumer)]
flat_consumers = [repl_hosts, *boot_consumers]
sync_obj = SyncObj(selfAddr, partners, consumers=flat_consumers)

PushManager.register('sync_obj', callable=lambda: sync_obj)

globals()['get_cluster_info'] = get_cluster_info

for k, v in m_globals.items():
    globals()[k] = v
    PushManager.register(k, callable=lambda q=k: globals()[q])

print(f"registering host: {sync_obj.selfNode.id}")
while not repl_hosts.tryAcquire(sync_obj.selfNode.id, data=host_resources, sync=True):
    print(f"waiting...")
    time.sleep(0.1)


# <<< setup sync obj

if web_app is not None:
    webserver = tornado.httpserver.HTTPServer(web_app)
    web_port = (base_port % 1000) + 11000
    print(f"starting webserver @ {web_port}")
    webserver.listen(web_port)


mgr_port = (base_port % 1000) + 50000
m, mt = serve_forever(mgr_port, b'password')

# tornado.ioloop.IOLoop.current().start()
loop = asyncio.get_event_loop()
try:
    loop.run_forever()
finally:
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()
    mt.join()
