from pushpy_examples.client.ex_push_manager import ExamplePushManager

m = ExamplePushManager()
m.connect()

# print on connected 'local' host
local_tasks = m.local_tasks()
local_tasks.apply(lambda: print("hello from local!"))

# print on all hosts (host_id is in scope when running on the remote host)
local_tasks = m.repl_tasks()
local_tasks.apply(lambda: print(f"hello from {host_id}!"))

