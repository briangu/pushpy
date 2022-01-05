import dill

from push.push_manager import PushManager
import sys

m = PushManager(address=('', int(sys.argv[1])), authkey=b'password')
m.connect()


class BatchProcess:
    def apply(self, k):
        import math
        return math.pi * k


class BatchProcess2:
    def apply(self, k):
        import math
        return math.pi + k

# TODO: test storing result and using it in a subsequent task
# TODO: add support for daemon deployment via repl task
repl_tasks = m.repl_tasks()
local_tasks = m.local_tasks()

def test_code(expected, key, *args, **kwargs):
    assert expected == repl_tasks.apply(key, *args, **kwargs, sync=True)
    assert expected == local_tasks.apply(key, *args, **kwargs)


repl_code_store = m.repl_code_store()
repl_code_store.set("batch_process", dill.dumps(BatchProcess), sync=True)
[test_code(BatchProcess().apply(i), "batch_process", i) for i in range(2)]
repl_code_store.set("batch_process", dill.dumps(BatchProcess2), sync=True)
[test_code(BatchProcess2().apply(i), "batch_process", i) for i in range(2)]
v = repl_code_store.get_head()
repl_code_store.set_head(v - 1, sync=True)
[test_code(BatchProcess().apply(i), "batch_process", i) for i in range(2)]
repl_code_store.set_head(v, sync=True)

def do_pi(k):
    import math
    return math.pi * k

v = repl_code_store.get_head()
# repl_code_store.inc_version_sync()
# assert repl_code_store.get_max_version() == v + 1
repl_code_store.set("my_lambda", dill.dumps(do_pi), sync=True)
# repl_code_store.commit_sync()
assert repl_code_store.get_head() == v + 1
[test_code(do_pi(i), "my_lambda", i) for i in range(2)]

my_lambda = lambda x: x*2

v = repl_code_store.get_head()
# repl_code_store.inc_version_sync()
repl_code_store.set("my_lambda", dill.dumps(my_lambda), sync=True)
# repl_code_store.commit_sync()
assert repl_code_store.get_head() == v + 1

[test_code(my_lambda(i), "my_lambda", i) for i in range(2)]

v = repl_code_store.get_head()
repl_code_store.set_head(v - 1, sync=True)
assert repl_code_store.get_head() == v - 1
v = repl_code_store.get_head()
[test_code(do_pi(i), "my_lambda", i) for i in range(2)]

# expect None response
assert repl_tasks.apply("my_lambda", 3) is None
assert repl_tasks.apply("my_lambda", 4) is None

# TODO: can create tool that loads/saves directory path into store
for k in repl_code_store.keys():
#    print(k, repl_code_store.get(k))
    print(k)


