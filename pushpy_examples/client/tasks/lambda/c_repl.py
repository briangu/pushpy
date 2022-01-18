from pushpy_examples.client.ex_push_manager import ExamplePushManager

m = ExamplePushManager()
m.connect()

# print on all hosts (host_id is in scope when running on the remote host)
repl_tasks = m.repl_tasks()
repl_tasks.apply(lambda: print(f"hello from {host_id}!"))

# execute a simple lambda on the host
print(repl_tasks.apply(lambda: 1 + 1, sync=True))


def do_pi(k):
    import math
    return math.pi * k


repl_code_store = m.repl_code_store()

# store a class as a lambda and run it
repl_code_store.set("my_lambda", do_pi, sync=True)
print(repl_tasks.apply("my_lambda", 2, sync=True))

# redefine lambda and run it
repl_code_store.set("my_lambda", lambda x: x * 2, sync=True)
print(repl_tasks.apply("my_lambda", 2, sync=True))
