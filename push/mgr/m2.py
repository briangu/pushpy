import dill

from push.mgr.qm import QueueManager

m = QueueManager(address=('', 50000), authkey=b'password')
m.connect()

QueueManager.register('do_add')
QueueManager.register('do_register')
QueueManager.register('do_lambda')

da = m.do_add()
dr = m.do_register()
dl = m.do_lambda()

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
