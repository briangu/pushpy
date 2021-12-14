from push.mgr.qm import QueueManager

m = QueueManager(address=('', 50000), authkey=b'password')
m.connect()

QueueManager.register('do_add')

da = m.do_add()

print("result = " + str(da.apply(1, 2)))
