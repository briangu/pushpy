import dill

from push.push_manager import PushManager
import sys

m = PushManager(address=('', int(sys.argv[1])), authkey=b'password')
m.connect()


local_tasks = m.local_tasks()

print(local_tasks.apply(dill.dumps(lambda: list(globals().keys()))))


