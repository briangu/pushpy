#!../../venv/bin/python3

from push.push_manager import PushManager
import sys

m = PushManager(address=('', int(sys.argv[1])), authkey=b'password')
m.connect()

ts = m.repl_ts()
print(m.repl_ts().flatten())

dt = m.local_tasks()
r = dt.apply(src=lambda *args, **kwargs: repl_ts.flatten())
print(r)

# import dill
#
# from push.mgr.push_manager import PushManager
# import sys
#
# m = PushManager(address=('', int(sys.argv[1])), authkey=b'password')
# m.connect()
#
#
# class BatchProcess:
#     def __init__(self, _ts=None):
#         global repl_ts
#         self.ts = _ts or repl_ts
#
#     def apply(self):
#         flat_ts = self.ts.flatten()
#         print(flat_ts)
#         return flat_ts
#
#
# ts = m.repl_ts()
# dt = m.local_tasks()
# r = dt.apply(dill.dumps(BatchProcess))
# print(r)
#
# print(m.repl_ts().flatten())
#
# r = dt.apply(src=dill.dumps(lambda *args, **kwargs: repl_ts.flatten()))
# print(r)
