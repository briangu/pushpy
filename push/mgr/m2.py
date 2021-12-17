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


print(list(QueueManager._registry.keys()))

sync_obj = m.sync_obj()
sync_obj.tryAcquireLock("/dog")
time.sleep(0.1)
print(sync_obj.isOwned("/dog"))
print(sync_obj.release("/dog"))
time.sleep(0.1)
print(sync_obj.isOwned("/dog"))

drc = m.do_register_callback()


class DoOnAcquire:
    def __call__(self, *args, **kwargs):
        # import traceback
        # traceback.print_stack()
        print(f"DoOnAcquire: {args} {kwargs}")

drc.apply("acquire", dill.dumps(DoOnAcquire))
sync_obj.tryAcquireLock("/dog")
time.sleep(0.1)
print(sync_obj.release("/dog"))

drc.apply("acquire", dill.dumps(lambda *args, **kwargs: print(f"acquire lambda: {args} {kwargs}")))
drc.apply("release", dill.dumps(lambda *args, **kwargs: print(f"release lambda: {args} {kwargs}")))
sync_obj.tryAcquireLock("/dog")
time.sleep(0.1)
print(sync_obj.release("/dog"))
