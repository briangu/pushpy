import dill

from push.mgr.qm import QueueManager
import sys

m = QueueManager(address=('', int(sys.argv[1])), authkey=b'password')
m.connect()

jq = m.get_job_queue()
rq = m.get_result_queue()
kvstore = m.kvstore()

class DoDaemonTask:
    def __init__(self, _jq=None, _rq=None):
        global job_queue
        global result_queue
        self.jq = _jq or job_queue
        self.rq = _rq or result_queue

    def apply(self, control):
        print("daemon here! 1")

        while control.running:
            self.jq.on_put.wait(1)
            job = self.jq.get_sync()
            if job is None:
                continue
            print(f"daemon queue size: {self.jq.qsize()}")

            print(f"daemon here! 2 job={job}")
            op = job[0]

            if op == 'add':
                print("daemon here! add")
                res = job[1] + job[2]
            elif op == 'sub':
                print("daemon here! sub")
                res = job[1] - job[2]
            elif op == 'exit':
                print(f"exiting op")
                return
            else:
                print("daemon here! type")
                res = op(job[1], job[2])

            print("daemon Sending result: " + str(res))
            self.rq.put(res)

        print("exiting")

kvstore.set_sync("my_daemon_task", dill.dumps(DoDaemonTask))

dt = m.tasks()
dt.stop("mdt")
dt.run("daemon", src="kvstore:my_daemon_task", name="mdt")
