import time

import dill

from timeseries.data_generator import DataGeneratorTask
from push.push_manager import PushManager
import sys

m = PushManager(address=('', int(sys.argv[1])), authkey=b'password')
m.connect()

ts = m.repl_ts().reset()

kvstore = m.repl_kvstore()
kvstore.set_sync("my_daemon_task", dill.dumps(DataGeneratorTask))

dt = m.local_tasks()
dt.stop("mdt")
dt.run("daemon", src="kvstore:my_daemon_task", name="mdt")

time.sleep(300)

dt.stop("mdt")
