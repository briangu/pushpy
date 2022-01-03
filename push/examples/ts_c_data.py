import time

import dill

from push.push_manager import PushManager
import sys

m = PushManager(address=('', int(sys.argv[1])), authkey=b'password')
m.connect()


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
