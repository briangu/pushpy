from push.examples.tasks.scope.show_context import ShowGlobals, ShowLocals
from push.push_manager import PushManager
import sys

m = PushManager(address=('', int(sys.argv[1])), authkey=b'password')
m.connect()


local_tasks = m.local_tasks()

class MainShowGlobals:
    def apply(self):
        return list(globals().keys())


print("Globals in the __main__ context")
print(local_tasks.apply(MainShowGlobals))
print()
print("globals in the ShowGlobals module")
print(local_tasks.apply(ShowGlobals))
print()
print("locals in the ShowLocals apply method")
print(local_tasks.apply(ShowLocals))
