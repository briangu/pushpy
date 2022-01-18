import time

from pushpy_examples.client.ex_push_manager import ExamplePushManager
from client.timeseries.data_generator import DataGeneratorTask

m = ExamplePushManager()
m.connect()

ts = m.repl_ts().reset()

repl_code_store = m.repl_code_store()
repl_code_store.set("my_daemon_task", DataGeneratorTask, sync=True)

dt = m.local_tasks()
dt.stop("mdt")
dt.run("daemon", src="my_daemon_task", name="mdt")

time.sleep(30)

dt.apply(lambda: print("done!"))

dt.stop("mdt")
