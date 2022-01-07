#!../../venv/bin/python3

from push.push_manager import PushManager
import sys

m = PushManager(address=('', int(sys.argv[1])), authkey=b'password')
m.connect()

ts = m.repl_ts()
print(m.repl_ts().flatten())

# dt = m.local_tasks()
# r = dt.apply(src=lambda *args, **kwargs: repl_ts.flatten())
# print(r)