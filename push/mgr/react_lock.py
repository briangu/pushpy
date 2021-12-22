import os
import socket
import threading
import time
import weakref

from pysyncobj import replicated
from pysyncobj.batteries import _ReplLockManagerImpl



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
