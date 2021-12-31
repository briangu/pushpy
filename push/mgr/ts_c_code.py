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


repl_code_store = m.repl_code_store()
repl_code_store.inc_version_sync()
repl_code_store.set_sync("batch_process", dill.dumps(BatchProcess))
repl_code_store.commit_sync()
print(repl_code_store.apply_sync("batch_process", 1))
print(repl_code_store.apply_sync("batch_process", 2))

def do_pi(k):
    import math
    return math.pi * k

v = repl_code_store.get_head()
repl_code_store.inc_version_sync()
assert repl_code_store.get_version() == v + 1
repl_code_store.set_sync("my_lambda", dill.dumps(do_pi))
repl_code_store.commit_sync()
assert repl_code_store.get_head() == v + 1
print(repl_code_store.apply_sync("my_lambda", 1))
print(repl_code_store.apply_sync("my_lambda", 2))

v = repl_code_store.get_head()
repl_code_store.inc_version_sync()
repl_code_store.set_sync("my_lambda", dill.dumps(lambda x: x*2))
repl_code_store.commit_sync()
assert repl_code_store.get_head() == v + 1
v = repl_code_store.get_head()
print(f"v={v}")
print(repl_code_store.apply_sync("my_lambda", 3))
print(repl_code_store.apply_sync("my_lambda", 4))
repl_code_store.set_head_sync(v - 1)
assert repl_code_store.get_head() == v - 1
v = repl_code_store.get_head()
print(f"v={v}")
print(repl_code_store.apply_sync("my_lambda", 3))
print(repl_code_store.apply_sync("my_lambda", 4))

# expect None response
assert repl_code_store.apply("my_lambda", 3) is None
assert repl_code_store.apply("my_lambda", 4) is None

