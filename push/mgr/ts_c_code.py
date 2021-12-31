import math

import dill

from push.mgr.push_manager import PushManager
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

repl_code_store = m.repl_code_store()
repl_code_store.commit_sync("batch_process", dill.dumps(BatchProcess))
assert BatchProcess().apply(1) == repl_tasks.apply_sync("batch_process", 1)
assert BatchProcess().apply(2) == repl_tasks.apply_sync("batch_process", 2)
repl_code_store.commit_sync([("batch_process", dill.dumps(BatchProcess2))])
assert BatchProcess2().apply(1) == repl_tasks.apply_sync("batch_process", 1)
assert BatchProcess2().apply(2) == repl_tasks.apply_sync("batch_process", 2)
v = repl_code_store.get_head()
repl_code_store.set_head_sync(v - 1)
assert BatchProcess().apply(1) == repl_tasks.apply_sync("batch_process", 1)
assert BatchProcess().apply(2) == repl_tasks.apply_sync("batch_process", 2)
repl_code_store.set_head_sync(v)

def do_pi(k):
    import math
    return math.pi * k

v = repl_code_store.get_head()
repl_code_store.inc_version_sync()
assert repl_code_store.get_version() == v + 1
repl_code_store.set_sync("my_lambda", dill.dumps(do_pi))
repl_code_store.commit_sync()
assert repl_code_store.get_head() == v + 1
assert do_pi(1) == repl_tasks.apply_sync("my_lambda", 1)
assert do_pi(2) == repl_tasks.apply_sync("my_lambda", 2)

my_lambda = lambda x: x*2

v = repl_code_store.get_head()
repl_code_store.inc_version_sync()
repl_code_store.set_sync("my_lambda", dill.dumps(my_lambda))
repl_code_store.commit_sync()
assert repl_code_store.get_head() == v + 1

assert my_lambda(3) == repl_tasks.apply_sync("my_lambda", 3)
assert my_lambda(4) == repl_tasks.apply_sync("my_lambda", 4)

v = repl_code_store.get_head()
repl_code_store.set_head_sync(v - 1)
assert repl_code_store.get_head() == v - 1
v = repl_code_store.get_head()
assert do_pi(3) == repl_tasks.apply_sync("my_lambda", 3)
assert do_pi(4) == repl_tasks.apply_sync("my_lambda", 4)

# expect None response
assert repl_tasks.apply("my_lambda", 3) is None
assert repl_tasks.apply("my_lambda", 4) is None

