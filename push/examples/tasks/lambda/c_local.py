import sys

from push.push_manager import PushManager

m = PushManager(address=('', int(sys.argv[1])), authkey=b'password')
m.connect()

local_tasks = m.local_tasks()

# execute a simple lambda on the host
print(local_tasks.apply(lambda: 1 + 1))


def do_pi(k):
    import math
    return math.pi * k


repl_code_store = m.repl_code_store()

# store a class as a lambda and run it
repl_code_store.set("my_lambda", do_pi, sync=True)
print(local_tasks.apply("my_lambda", 2))

# redefine lambda and run it
repl_code_store.set("my_lambda", lambda x: x * 2, sync=True)
print(local_tasks.apply("my_lambda", 2))
