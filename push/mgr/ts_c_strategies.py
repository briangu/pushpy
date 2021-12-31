import sys
import time

import dill
import numpy as np

from push.mgr.host_resources import HostRequirements, GPURequirements
from push.mgr.push_manager import PushManager
from push.mgr.strategy import Strategy

m = PushManager(address=('', int(sys.argv[1])), authkey=b'password')
m.connect()

symbols = ['MSFT', 'TWTR', 'EBAY', 'CVX', 'W', 'GOOG', 'FB']
strategy_capabilities = ['CPU', 'GPU']
np.random.seed(0)


def process_ts_updates(idx_data, keys, data):
    cluster_size, partition_id, hr = get_cluster_info(repl_hosts, sync_obj)
    if cluster_size == 0:
        return
    print(f"post-processing: {idx_data} {keys} {cluster_size} {partition_id}")
    capable_strategies = [x for x in repl_strategies.rawData() if hr.has_capacity(x.requirements)]
    owned_strategies = [x for x in capable_strategies if hash(x) % cluster_size == partition_id]
    print(f"owned strategies: {[x.id for x in owned_strategies]}")
    sym_map = dict()
    for s in owned_strategies:
        for symbol in s.symbols:
            x = sym_map.get(symbol)
            if x is None:
                x = set()
                sym_map[symbol] = x
            x.add(s)
    for symbol in keys:
        strategies = sym_map.get(symbol)
        # if strategies is not None:
        #     print(f"applying strategies: {[x.id for x in strategies]}")


repl_code_store = m.repl_code_store()
repl_code_store.add("process_ts_updates", dill.dumps(process_ts_updates))


def random_host_requirement():
    return HostRequirements(
        cpu=None,
        memory=None,
        gpu=GPURequirements(count=np.random.randint(0, 2))
    )


strategies = [Strategy(id=i, name=f"s_{i}", symbols=np.random.choice(symbols, 2), requirements=random_host_requirement()) for i in range(10)]

repl_strategies = m.repl_strategies()
repl_strategies.reset(strategies)


class DataGeneratorTask:
    def __init__(self, _ts=None):
        global repl_ts
        self.ts = _ts or repl_ts

    def apply(self, control):
        print("daemon here! 1")

        import datetime
        from datetime import timezone
        import random
        import time

        while control.running:
            symbols = ['MSFT', 'TWTR', 'EBAY', 'CVX', 'W', 'GOOG', 'FB']
            now = datetime.datetime.now(timezone.utc)
            d = [random.uniform(10, 100) for _ in symbols]
            self.ts.append(now, symbols, d)
            time.sleep(1)


ts = m.repl_ts().reset()

kvstore = m.repl_kvstore()
kvstore.set_sync("my_daemon_task", dill.dumps(DataGeneratorTask))

dt = m.local_tasks()
dt.stop("mdt")
dt.run("daemon", src="kvstore:my_daemon_task", name="mdt")

time.sleep(300)

dt.stop("mdt")
