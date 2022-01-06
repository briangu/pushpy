import sys
import time

import numpy as np

from push.push_manager import PushManager
from push.examples.timeseries.data_generator import DataGeneratorTask

m = PushManager(address=('', int(sys.argv[1])), authkey=b'password')
m.connect()

symbols = ['MSFT', 'TWTR', 'EBAY', 'CVX', 'W', 'GOOG', 'FB']
strategy_capabilities = ['CPU', 'GPU']
np.random.seed(0)


def process_ts_updates(idx_data, keys, data):
    print(f"processing: idx={idx_data} keys={keys!r}")


repl_code_store = m.repl_code_store()
repl_code_store.update({"process_ts_updates": process_ts_updates}, sync=True)

ts = m.repl_ts().reset()

repl_code_store = m.repl_code_store()
repl_code_store.set("my_daemon_task", DataGeneratorTask, sync=True)

dt = m.local_tasks()
dt.stop("mdt")
dt.clear_events()
dt.run("daemon", src="my_daemon_task", name="mdt")

time.sleep(300)

dt.stop("mdt")
