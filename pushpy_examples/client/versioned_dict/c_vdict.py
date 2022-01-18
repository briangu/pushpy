from pushpy_examples.client.ex_push_manager import ExamplePushManager

m = ExamplePushManager()
m.connect()

local_tasks = m.local_tasks()

repl_ver_store = m.repl_ver_store()
repl_ver_store.clear()

repl_ver_store.update({
    "a": 1,
    "b": 2,
    "c": 3
}, sync=True)

print(list(repl_ver_store.keys()))
# run a lambda to enumerate items
print(local_tasks.apply(lambda: list(repl_ver_store.items())))

# delete a key
print("delete b")
repl_ver_store.delete("b", sync=True)
# print(list(repl_ver_store.keys()))
print(local_tasks.apply(lambda: list(repl_ver_store.items())))

print("add d")
repl_ver_store.set("d", 4, sync=True)
# print(list(repl_ver_store.keys()))
print(local_tasks.apply(lambda: list(repl_ver_store.items())))

# using None for a value will "delete it"
print("delete a and update c")
repl_ver_store.update({
    "a": None,
    "c": 5,
}, sync=True)

# different ways of reading values
print({k: repl_ver_store.get(k) for k in repl_ver_store.keys()})

