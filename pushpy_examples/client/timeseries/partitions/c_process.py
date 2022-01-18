import time

import numpy as np

from pushpy_examples.client.ex_push_manager import ExamplePushManager
from client.timeseries.data_generator import DataGeneratorTask
from pushpy.host_resources import HostRequirements, GPURequirements
from client.timeseries.partitions.strategy import Strategy

m = ExamplePushManager()
m.connect()

symbols = ['MSFT', 'TWTR', 'EBAY', 'CVX', 'W', 'GOOG', 'FB']
strategy_capabilities = ['CPU', 'GPU']
np.random.seed(0)


def process_ts_updates(idx_data, keys, data):
    from boot_common import get_partition_info
    cluster_size, partition_id, host_resources = get_partition_info()
    if cluster_size == 0:
        return
    # print(f"post-processing: {idx_data} {keys} {cluster_size} {partition_id}")
    capable_strategies = [x for x in repl_strategies.rawData() if host_resources.has_capacity(x.requirements)]
    owned_strategies = [x for x in capable_strategies if hash(x) % cluster_size == partition_id]
    sym_map = dict()
    for s in owned_strategies:
        for symbol in s.symbols:
            x = sym_map.get(symbol)
            if x is None:
                x = set()
                sym_map[symbol] = x
            x.add(s)
    print(f"processing: idx={idx_data} strategies={[x.id for x in owned_strategies]!r}")
    # for symbol in keys:
    #     strategies = sym_map.get(symbol)
    #     # if strategies is not None:
    #     #     print(f"applying strategies: {[x.id for x in strategies]}")


repl_code_store = m.repl_code_store()
repl_code_store.update({"process_ts_updates": process_ts_updates}, sync=True)


def random_host_requirement():
    return HostRequirements(
        cpu=None,
        memory=None,
        gpu=GPURequirements(count=np.random.randint(0, 2))
    )


strategies = [Strategy(id=i, name=f"s_{i}", symbols=np.random.choice(symbols, 2), requirements=random_host_requirement()) for i in range(10)]

repl_strategies = m.repl_strategies()
repl_strategies.reset(strategies)

ts = m.repl_ts().reset()

repl_code_store = m.repl_code_store()
repl_code_store.set("my_daemon_task", DataGeneratorTask, sync=True)

dt = m.local_tasks()
dt.stop("mdt")
dt.clear_events()
dt.run("daemon", src="my_daemon_task", name="mdt")

time.sleep(300)

dt.stop("mdt")
