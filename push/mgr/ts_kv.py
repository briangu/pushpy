import random
import time

import dill

from push.mgr.qm import QueueManager
import sys

m = QueueManager(address=('', int(sys.argv[1])), authkey=b'password')
m.connect()


class DataGeneratorTask:
    def __init__(self, _ts=None):
        global repl_ts
        self.ts = _ts or repl_ts

    def apply(self, control):
        print("daemon here! 1")

        import datetime
        from datetime import timezone, timedelta
        import random
        import time

        while control.running:
            stocks = ['MSFT', 'GOOG', 'WDAY']
            now = datetime.datetime.now(timezone.utc)
            d = [random.uniform(10, 100) for _ in stocks]
            self.ts.append(now, stocks, d)
            time.sleep(1)

def mk_on_get_c(v):
    def on_get(self):
        self.write(f"lambda world! {v}")
        self.finish()
    return on_get

kvstore = m.kvstore()
kvstore.set_sync("my_daemon_task", dill.dumps(DataGeneratorTask))

on_get_v = kvstore.get("on_get_v")
if on_get_v is None:
    on_get_v = 0
on_get_v += 1
kvstore.set_sync("on_get_v", on_get_v)
kvstore.set_sync(f"on_get_v{on_get_v}", dill.dumps(mk_on_get_c(on_get_v)))


