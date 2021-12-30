import dill

from push.mgr.push_manager import PushManager
import sys

m = PushManager(address=('', int(sys.argv[1])), authkey=b'password')
m.connect()

# while jq.qsize() > 0:
#     jq.get_sync()
# while rq.qsize() > 0:
#     rq.get_sync()

class BatchProcess:
    def __init__(self, _jq=None, _rq=None):
        global job_queue
        global result_queue
        self.jq = _jq or job_queue
        self.rq = _rq or result_queue

    def apply(self):
        results = []
        for i in range(100):
            self.jq.put(('add', i, i*2))
            self.jq.put(('sub', i*2, i))
        i = 0
        while i < 100:
            rv = self.rq.get_sync()
            if rv is None:
                continue
            results.append(rv)
            i += 1
        return results

dl = m.apply_lambda()
r = dl.apply(src=dill.dumps(BatchProcess))
print(r)

jq = m.get_job_queue()
rq = m.get_result_queue()
bp = BatchProcess(jq, rq)
print(bp.apply())
