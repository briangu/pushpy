import multiprocessing
import threading
import time
import uuid
import weakref

import dill
from pysyncobj import SyncObj, replicated, replicated_sync
from pysyncobj.batteries import _ReplLockManagerImpl, ReplDict, ReplQueue

from push.mgr.qm import QueueManager
import psutil
import sys
import socket
import os


# from tensorflow.python.client import device_lib


# TODO: GPU enabled systems can have a GPU client attach and listen to a queue to do work
#       we can report if there's a client registered or not
#       Question: how do we register lambdas w/ the GPU client?  send serialized?
#                 how do we sync the answer with the request? request id?
#       if we host the GPU processing in this server, then we may need a lock to guard the GPU
def get_available_gpus():
    return []
    # local_device_protos = device_lib.list_local_devices()
    # return [x.name for x in local_device_protos if x.device_type == 'GPU']


# https://gist.github.com/spacecowboy/1203760

'''
Taken directly from the examples for multiprocessing. The only purpose for this
file is to serve two queues for clients, of which there are two. 
'''


class OnReplicate:
    handle_map = dict()

    def on_replicate(self, method, *args, **kwargs):
        if method in self.handle_map:
            self.handle_map[method](*args, **kwargs)


onrep = OnReplicate()


class MyReplLockManagerImpl(_ReplLockManagerImpl):
    def __init__(self, autoUnlockTime, on_event=None):
        super(MyReplLockManagerImpl, self).__init__(autoUnlockTime=autoUnlockTime)
        self.on_event = on_event

    @replicated
    def acquire(self, lockID, clientID, currentTime):
        # print("acquire", lockID, clientID, currentTime)
        super().acquire(lockID, clientID, currentTime, _doApply=True)
        if self.on_event is not None:
            self.on_event('acquire', lockID, clientID, currentTime)

    @replicated
    def release(self, lockID, clientID):
        super().release(lockID, clientID, _doApply=True)
        if self.on_event is not None:
            self.on_event('release', lockID, clientID)

    # def isOwned(self, lockPath, clientID, currentTime):
    #     existingLock = self.__locks.get(lockPath, None)
    #     # if self.__verbose:
    #     #     print(existingLock, clientID)
    #     if existingLock is not None:
    #         if existingLock[0] == clientID:
    #             if currentTime - existingLock[1] < self.__autoUnlockTime:
    #                 return True
    #     return False
    # def isOwned(self, lockID, clientID, currentTime):
    #     existingLock = self.__locks.get(lockID, None)
    #     if existingLock is not None:
    #         if existingLock[0] == clientID:
    #             if currentTime - existingLock[1] < self.__autoUnlockTime:
    #                 return True
    #     return False


class MyReplLockManager:
    def __init__(self, autoUnlockTime, selfID=None, on_event=None):
        # super().__init__(autoUnlockTime, selfID)
        self.__lockImpl = MyReplLockManagerImpl(autoUnlockTime=autoUnlockTime,
                                                on_event=on_event)
        if selfID is None:
            selfID = '%s:%d:%d' % (socket.gethostname(), os.getpid(), id(self))
        self.__selfID = selfID
        self.__autoUnlockTime = autoUnlockTime
        self.__mainThread = threading.current_thread()
        self.__initialised = threading.Event()
        self.__destroying = False
        self.__lastProlongateTime = 0
        self.__thread = threading.Thread(target=MyReplLockManager._autoAcquireThread, args=(weakref.proxy(self),))
        self.__thread.start()
        while not self.__initialised.is_set():
            pass

    def _consumer(self):
        return self.__lockImpl

    def destroy(self):
        """Destroy should be called before destroying ReplLockManager"""
        self.__destroying = True

    def _autoAcquireThread(self):
        self.__initialised.set()
        try:
            while True:
                if not self.__mainThread.is_alive():
                    break
                if self.__destroying:
                    break
                time.sleep(0.1)
                if time.time() - self.__lastProlongateTime < float(self.__autoUnlockTime) / 4.0:
                    continue
                syncObj = self.__lockImpl._syncObj
                if syncObj is None:
                    continue
                if syncObj._getLeader() is not None:
                    self.__lastProlongateTime = time.time()
                    self.__lockImpl.prolongate(self.__selfID, time.time())
        except ReferenceError:
            pass

    def tryAcquire(self, lockID, callback=None, sync=False, timeout=None):
        """Attempt to acquire lock.

        :param lockID: unique lock identifier.
        :type lockID: str
        :param sync: True - to wait until lock is acquired or failed to acquire.
        :type sync: bool
        :param callback: if sync is False - callback will be called with operation result.
        :type callback: func(opResult, error)
        :param timeout: max operation time (default - unlimited)
        :type timeout: float
        :return True if acquired, False - somebody else already acquired lock
        """
        attemptTime = time.time()
        if sync:
            acquireRes = self.__lockImpl.acquire(lockID, self.__selfID, attemptTime, callback=callback, sync=sync, timeout=timeout)
            acquireTime = time.time()
            if acquireRes:
                if acquireTime - attemptTime > self.__autoUnlockTime / 2.0:
                    acquireRes = False
                    self.__lockImpl.release(lockID, self.__selfID, sync=sync)
            return acquireRes

        def asyncCallback(acquireRes, errCode):
            if acquireRes:
                acquireTime = time.time()
                if acquireTime - attemptTime > self.__autoUnlockTime / 2.0:
                    acquireRes = False
                    self.__lockImpl.release(lockID, self.__selfID, sync=False)
            callback(acquireRes, errCode)

        self.__lockImpl.acquire(lockID, self.__selfID, attemptTime, callback=asyncCallback, sync=sync, timeout=timeout)

    def isAcquired(self, lockID):
        """Check if lock is acquired by ourselves.

        :param lockID: unique lock identifier.
        :type lockID: str
        :return True if lock is acquired by ourselves.
         """
        return self.__lockImpl.isAcquired(lockID, self.__selfID, time.time())

    def release(self, lockID, callback=None, sync=False, timeout=None):
        """
        Release previously-acquired lock.

        :param lockID:  unique lock identifier.
        :type lockID: str
        :param sync: True - to wait until lock is released or failed to release.
        :type sync: bool
        :param callback: if sync is False - callback will be called with operation result.
        :type callback: func(opResult, error)
        :param timeout: max operation time (default - unlimited)
        :type timeout: float
        """
        self.__lockImpl.release(lockID, self.__selfID, callback=callback, sync=sync, timeout=timeout)


lock_mgr = MyReplLockManager(10, on_event=onrep.on_replicate)


# lock_mgr = ReplLockManager(10)

class MyReplDict(ReplDict):

    @replicated_sync
    def set_sync(self, key, value):
        self.set(key, value, _doApply=True)


class MyReplQueue(ReplQueue):

    on_put = threading.Event()

    @replicated
    def put(self, item):
        super().put(item, _doApply=True)
        print(f"queue: put {item}")
        self.on_put.set()

    @replicated_sync
    def put_sync(self, item):
        self.put(item, _doApply=True)

    @replicated_sync
    def get_sync(self, default=None):
        return self.get(default=default, _doApply=True)


kvstore = MyReplDict()

# Define two queues, one for putting jobs on, one for putting results on.
job_queue = MyReplQueue()  # Queue.Queue()
result_queue = MyReplQueue()
print(job_queue)
print(result_queue)

selfAddr = sys.argv[1]  # "localhost:10000"
partners = sys.argv[2:]  # ["localhost:10001", "localhost:10002"]
sync_lock = SyncObj(selfAddr, partners, consumers=[lock_mgr, kvstore, job_queue, result_queue])


class DoAdd:
    def apply(self, x, y):
        print(threading.current_thread().ident)
        return x + y


da = DoAdd()


class DoRegister:
    def apply(self, name, src):
        src = dill.loads(src)
        q = src()
        QueueManager.register(name, callable=lambda: q)


dr = DoRegister()


class DoRegisterCallback:
    def apply(self, name, src):
        global onrep
        src = dill.loads(src)
        if isinstance(src, type):
            q = src()
            onrep.handle_map[name] = q.apply if hasattr(q, 'apply') else q
        else:
            onrep.handle_map[name] = src


drc = DoRegisterCallback()
QueueManager.register("do_register_callback", callable=lambda: drc)


def load_src(src):
    if isinstance(src, str):
        print(f"load_src: {src}")
        p = src.split(":")
        src = kvstore.get(p[1])
        if src is None:
            return None
        src = dill.loads(src)
    else:
        print(f"load_src: code")
        src = dill.loads(src)
    print(f"load_src: {type(src)}")
    if isinstance(src, type):
        src = src()
        src = src.apply if hasattr(src, 'apply') else src
    return src


class DoLambda:
    def apply(self, src, *args, **kwargs):
        src = load_src(src)
        return src(*args, **kwargs)


dl = DoLambda()


class DoRegistry:
    def apply(self):
        return list(QueueManager._registry.keys())


dreg = DoRegistry()


class DoLocaleCapabilities:
    def apply(self):
        return {
            'cpu_count': multiprocessing.cpu_count(),
            'virtual_memory': psutil.virtual_memory(),
            'GPUs': get_available_gpus()
        }


class DoKvStore():
    def set(self, k, v):
        global kvstore
        # kvstore.set(k, v)
        print(k, v)

    def get(self, k):
        global kvstore
        # return kvstore.get(k)
        print(k)


dkvs = DoKvStore()


class TaskControl:
    running = True

class TaskContext:
    def __init__(self, control, thread):
        self.control = control
        self.thread = thread

class DoTask:
    task_threads = dict()

    def run(self, task_type, src, name=None):
        src = load_src(src)
        name = name or str(uuid.uuid4())
        if name in self.task_threads:
            raise RuntimeError(f"task already running: {name}")
        if task_type == "daemon":
            task_control = TaskControl()
            task_context = TaskContext(task_control,
                                       threading.Thread(target=src, args=(task_control,)))
            task_context.thread.start()
            self.task_threads[name] = task_context
            print(self.task_threads)

    def stop(self, name):
        if name not in self.task_threads:
            return
        self.task_threads[name].control.running = False
        self.task_threads[name].thread.join(timeout=10)
        del self.task_threads[name]

dotask = DoTask()

dlc = DoLocaleCapabilities()

QueueManager.register('get_job_queue', callable=lambda: job_queue)
QueueManager.register('get_result_queue', callable=lambda: result_queue)
QueueManager.register('do_add', callable=lambda: da)

QueueManager.register('do_register', callable=lambda: dr)
QueueManager.register('apply_lambda', callable=lambda: dl)
QueueManager.register('get_registry', callable=lambda: dreg)
QueueManager.register('sync_obj', callable=lambda: lock_mgr)
QueueManager.register('kvstore', callable=lambda: kvstore)
QueueManager.register('tasks', callable=lambda: dotask)
QueueManager.register('locale_capabilities', callable=lambda: dlc)

# Start up
mgr_port = (int(sys.argv[1].split(":")[1]) % 1000) + 50000
print(mgr_port)
m = QueueManager(address=('', mgr_port), authkey=b'password')
s = m.get_server()
# TODO: i think this code can be rewritten to use asyncio / twisted
s.serve_forever()
