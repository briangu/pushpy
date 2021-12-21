import dill
import time

from push.mgr.qm import QueueManager

m = QueueManager(address=('', 50000), authkey=b'password')
m.connect()

print(f"locale capabilities: {m.locale_capabilities().apply()}")

da = m.do_add()
dr = m.do_register()
dl = m.apply_lambda()

print("do_add result = " + str(da.apply(1, 2)))


class Do_woot:
    def apply(self):
        return "woot"

x = dill.dumps(Do_woot)
# print(type(x))

dr.apply('do_test', x)

QueueManager.register('do_test')

dt = m.do_test()

print("do_test result = " + str(dt.apply()))


print(f"do_lambda result={str(dl.apply(src=x))}")
print(f"raw lambda result={str(dl.apply(src=dill.dumps(lambda *args, **kwargs: 'lambda woot!')))}")

# print(f"raw lambda result={str(dl.apply(src=dill.dumps(lambda *args, **kwargs: str(job_queue))))}")


print(list(QueueManager._registry.keys()))

sync_obj = m.sync_obj()
print(sync_obj.tryAcquire("/dog", sync=True))
time.sleep(0.1)
for i in range(10):
    time.sleep(0.1)
    ia = sync_obj.isAcquired("/dog")
    if ia:
        break
    print(sync_obj.isAcquired("/dog"))
print(sync_obj.release("/dog"))
time.sleep(0.1)
print(sync_obj.isAcquired("/dog"))

drc = m.do_register_callback()


class DoOnAcquire:
    def __call__(self, *args, **kwargs):
        # import traceback
        # traceback.print_stack()
        print(f"DoOnAcquire: {args} {kwargs}")

drc.apply("acquire", dill.dumps(DoOnAcquire))

sync_obj.tryAcquire("/dog", sync=True)
time.sleep(0.1)
print(sync_obj.release("/dog"))

drc.apply("acquire", dill.dumps(lambda *args, **kwargs: print(f"acquire lambda: {args} {kwargs}")))
drc.apply("release", dill.dumps(lambda *args, **kwargs: print(f"release lambda: {args} {kwargs}")))
sync_obj.tryAcquire("/dog", sync=True)
time.sleep(0.1)
print(sync_obj.release("/dog"))

kvstore = m.kvstore()
print(kvstore)

kvstore.set_sync(key="my_lambda", value=dill.dumps(lambda *args, **kwargs: f"my lambda {args} {kwargs}"))
# while True:
#     if kvstore.get("my_lambda") is not None:
#         break
# kvstore.set("my_lambda", "foo")

print(dl.apply(src="kvstore:my_lambda"))

# TODO: task management
#   cron https://github.com/dbader/schedule
#   lambda - see above
#   daemon - start thread (or process?) and run task inside
#    pmap - parallel map execution of lambdas across a set of locales
#   broadcast / report (map reduce?)
#   on_replicate - use URN to reference kvstore
#
#   how do we collect the stdio from the tasks?
#
# class DoLambdaQueueTask:
#
#     def __init__(self, _jq=None, _rq=None):
#         global job_queue
#         global result_queue
#         self.jq = _jq or job_queue
#         self.rq = _rq or result_queue
#
#     def apply(self):
#         print("here! 1")
#
#         self.jq.put_sync(('add', 2, 3))
#         job = self.jq.get_sync()
#         while job is None:
#             time.sleep(0.1)
#             job = self.jq.get_sync()
#         if job is None:
#             print(f"no job, returning")
#             return
#
#         print(f"here! 2 job={job}")
#         op = job[0]
#
#         if op == 'add':
#             print("here! add")
#             res = job[1] + job[2]
#         elif op == 'sub':
#             print("here! sub")
#             res = job[1] - job[2]
#         else:
#             print("here! type")
#             res = op(job[1], job[2])
#
#         print(f"here! res = {res}")
#         # print("Sending result: " + str(res))
#         self.rq.put_sync(res)
#         rv = self.rq.get_sync()
#         print(f"result queue: {rv}")
#         return rv
#
# kvstore.set_sync("my_task", dill.dumps(DoLambdaQueueTask))
#
# dl.apply(src="kvstore:my_task")

jq = m.get_job_queue()
rq = m.get_result_queue()

# dqt = DoLambdaQueueTask(m.get_job_queue(), m.get_result_queue())
# dqt.apply()

class DoDaemonTask:
    def __init__(self, _jq=None, _rq=None):
        global job_queue
        global result_queue
        self.jq = _jq or job_queue
        self.rq = _rq or result_queue

    def apply(self, control):
        print("daemon here! 1")

        while control.running:
            # job = self.jq.get_sync()
            #
            # print(f"daemon here! 2 job={job}")
            # if job is None:
            #     time.sleep(0.1)
            #     continue
            #
            # op = job[0]
            #
            # if op == 'add':
            #     print("daemon here! add")
            #     res = job[1] + job[2]
            # elif op == 'sub':
            #     print("daemon here! sub")
            #     res = job[1] - job[2]
            # else:
            #     print("daemon here! type")
            #     res = op(job[1], job[2])
            #
            # print(f"daemon here! res = {res}")
            # # print("Sending result: " + str(res))
            # self.rq.put_sync(res)
            # print("here! 1")
            # if self.jq.qsize() == 0:
            #     self.jq.put_sync(('add', 2, 3))
            job = self.jq.get_sync()
            if job is None:
                # print(f"no job, returning")
                continue
            print(f"queue size: {self.jq.qsize()}")

            print(f"here! 2 job={job}")
            op = job[0]

            if op == 'add':
                print("here! add")
                res = job[1] + job[2]
            elif op == 'sub':
                print("here! sub")
                res = job[1] - job[2]
            else:
                print("here! type")
                res = op(job[1], job[2])

            print(f"here! res = {res}")
            # print("Sending result: " + str(res))
            self.rq.put_sync(res)
            # rv = self.rq.get_sync()
            # print(f"result queue: {rv}")
            # return rv

        print("exiting")

kvstore.set_sync("my_daemon_task", dill.dumps(DoDaemonTask))

dt = m.tasks()
dt.stop("mdt")
dt.run("daemon", src="kvstore:my_daemon_task", name="mdt")

jq.put_sync(('add', 5, 6))
# while rq.qsize() == 0:
#     time.sleep(0.1)
time.sleep(1)
rv = rq.get_sync()
print(f"result queue: {rv}")
time.sleep(1)

dt.stop("mdt")
