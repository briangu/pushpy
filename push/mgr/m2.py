from push.mgr.qm import QueueManager

QueueManager.register('do_add')

m = QueueManager(address=('', 50000), authkey=b'password')
m.connect()

da = m.do_add()

print("result = " + str(da.apply(1, 2)))
