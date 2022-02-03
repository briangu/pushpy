import time

from pushpy_examples.client.ex_push_manager import ExamplePushManager
from pushpy_examples.client.timeseries.data_generator import DataGeneratorTask

m = ExamplePushManager()
m.connect()


def process_ts_updates(idx_data, keys, data):
    print(f"processing: idx={idx_data} keys={keys!r}")

# reset the time series
ts = m.repl_ts().reset()

# setup the process task and the data generator
repl_code_store = m.repl_code_store()
repl_code_store.set("process_ts_updates", process_ts_updates, sync=True)
repl_code_store.set("my_daemon_task", DataGeneratorTask, sync=True)

dt = m.local_tasks()
dt.stop("mdt")
dt.clear_events()
dt.run("daemon", src="my_daemon_task", name="mdt")

time.sleep(30)

dt.stop("mdt")
