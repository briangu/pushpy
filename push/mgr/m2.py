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


print(list(QueueManager._registry.keys()))

sync_lock = m.sync_lock()
sync_lock.tryAcquireLock("/dog")
time.sleep(0.1)
print(sync_lock.isOwned("/dog"))
print(sync_lock.release("/dog"))
time.sleep(0.1)
print(sync_lock.isOwned("/dog"))

drc = m.do_register_callback()


class DoOnAcquire:
    def apply(self, p, c, t):
        import traceback
        traceback.print_stack()
        print(f"DoOnAcquire: {p}, {c}, {t}")

drc.apply("acquire", dill.dumps(DoOnAcquire))
sync_lock.tryAcquireLock("/dog")
time.sleep(0.1)
print(sync_lock.release("/dog"))

drc.apply("acquire", dill.dumps(lambda p,c,t: print(f"DoOnAcquire lambda: {p}, {c}, {t}")))
sync_lock.tryAcquireLock("/dog")
time.sleep(0.1)
print(sync_lock.release("/dog"))
