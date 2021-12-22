import random

import dill

from push.mgr.qm import QueueManager
import sys

m = QueueManager(address=('', int(sys.argv[1])), authkey=b'password')
m.connect()

ts = m.ts()
print(m.ts().flatten())

dl = m.apply_lambda()
r = dl.apply(src=dill.dumps(lambda *args, **kwargs: repl_ts.flatten()))
print(r)
