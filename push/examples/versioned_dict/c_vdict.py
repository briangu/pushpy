import sys

from push.push_manager import PushManager

m = PushManager(address=('', int(sys.argv[1])), authkey=b'password')
m.connect()

repl_ver_store = m.repl_ver_store()
repl_ver_store.update({
    "a": 1,
    "b": 2,
    "c": 3
}, sync=True)

print(list(repl_ver_store.keys()))

repl_ver_store.delete("b", sync=True)
repl_ver_store.set("d", 4, sync=True)

print(list(repl_ver_store.keys()))
print({k: repl_ver_store.get(k) for k in repl_ver_store.keys()})

local_tasks = m.local_tasks()
print(local_tasks.apply(lambda: list(repl_ver_store.items())))

