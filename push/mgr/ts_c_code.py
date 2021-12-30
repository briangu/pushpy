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
repl_code_store.set_sync("batch_process", dill.dumps(BatchProcess))
print(repl_code_store.apply_sync("batch_process", 1))
print(repl_code_store.apply_sync("batch_process", 2))

def do_pi(k):
    import math
    return math.pi * k

repl_code_store.set_sync("my_lambda", dill.dumps(do_pi))
print(repl_code_store.apply_sync("my_lambda", 1))
print(repl_code_store.apply_sync("my_lambda", 2))

