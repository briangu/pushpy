import sys
import time

from push.examples.timeseries.data_generator import DataGeneratorTask
from push.push_manager import PushManager

m = PushManager(address=('', int(sys.argv[1])), authkey=b'password')
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
