import random

import dill

from push.mgr.qm import QueueManager
import sys

m = QueueManager(address=('', int(sys.argv[1])), authkey=b'password')
m.connect()


class BatchProcess:
    def __init__(self, _ts=None):
        global repl_ts
        self.ts = _ts or repl_ts

    def apply(self):
        import datetime
        from datetime import timezone, timedelta
        import random

        stocks = ['MSFT', 'GOOG', 'WDAY']
        now = datetime.datetime.now(timezone.utc)
        for i in range(100, 1, -1):
            t = now - timedelta(days=i)
            d = [random.uniform(10, 100) for _ in stocks]
            self.ts.append(t, stocks, d)

ts = m.ts()
dl = m.apply_lambda()
dl.apply(src=dill.dumps(BatchProcess))

# ts = m.ts()
#
# stocks = ['MSFT', 'GOOG', 'WDAY']
# now = datetime.datetime.now(timezone.utc)
# for i in range(100, 0, -1):
#     t = now - timedelta(days=i)
#     s = [random.choice(stocks)]
#     d = [random.uniform(10, 100)]
#     ts.append(t, s, d)

print(m.ts().flatten())

r = dl.apply(src=dill.dumps(lambda *args, **kwargs: repl_ts.flatten()))
print(r)
