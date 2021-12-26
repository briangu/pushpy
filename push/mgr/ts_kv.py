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


kvstore = m.kvstore()
kvstore.set_sync("my_daemon_task", dill.dumps(DataGeneratorTask))
