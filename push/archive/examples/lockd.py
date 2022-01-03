#!/usr/bin/env python
from __future__ import print_function

import sys
import threading
import weakref
import time

sys.path.append("../../")
from pysyncobj import SyncObj, replicated, SyncObjConf
import asyncio
import sys
import tornado.web


class LockImpl(SyncObj):
    def __init__(self, selfAddress, partnerAddrs, autoUnlockTime, conf):
        super(LockImpl, self).__init__(selfAddress, partnerAddrs, conf=conf)
        self.__selfClientID = selfAddress
        self.__locks = {}
        self.__autoUnlockTime = autoUnlockTime
        self.__verbose = True

    @replicated
    def acquire(self, lockPath, clientID, currentTime):
        if self.__verbose:
            print(f"{threading.get_ident()} acquire: {lockPath}, {clientID}, {currentTime}")
        existingLock = self.__locks.get(lockPath, None)
        # Auto-unlock old lock
        if existingLock is not None:
            if currentTime - existingLock[1] > self.__autoUnlockTime:
                existingLock = None
        # Acquire lock if possible
        if existingLock is None or existingLock[0] == clientID:
            self.__locks[lockPath] = (clientID, currentTime)
            return True
        # Lock already acquired by someone else
        return False

    @replicated
    def ping(self, clientID, currentTime):
        # if self.__verbose:
        #     print(f"ping: {clientID}, {currentTime}, {self.__locks}")
        for lockPath in list(self.__locks.keys()):
            lockClientID, lockTime = self.__locks[lockPath]

            if currentTime - lockTime > self.__autoUnlockTime:
                del self.__locks[lockPath]
                continue

            if lockClientID == clientID:
                self.__locks[lockPath] = (clientID, currentTime)

    @replicated
    def release(self, lockPath, clientID):
        if self.__verbose:
            print(f"{threading.get_ident()} release: {lockPath} {clientID}")
        existingLock = self.__locks.get(lockPath, None)
        if existingLock is not None and existingLock[0] == clientID:
            del self.__locks[lockPath]

    @replicated
    def toggle_verbose(self):
        self.__verbose = not self.__verbose
        print(f"{threading.get_ident()} verbose: {self.__verbose}")

    def isOwned(self, lockPath, clientID, currentTime):
        existingLock = self.__locks.get(lockPath, None)
        # if self.__verbose:
        #     print(existingLock, clientID)
        if existingLock is not None:
            if existingLock[0] == clientID:
                if currentTime - existingLock[1] < self.__autoUnlockTime:
                    return True
        return False

    def isAcquired(self, lockPath, clientID, currentTime):
        existingLock = self.__locks.get(lockPath, None)
        # if self.__verbose:
        #     print(existingLock, clientID)
        if existingLock is not None:
            if currentTime - existingLock[1] < self.__autoUnlockTime:
                return True
        return False


class Lock(object):
    def __init__(self, selfAddress, partnerAddrs, autoUnlockTime, conf=None):
        self.__lockImpl = LockImpl(selfAddress, partnerAddrs, autoUnlockTime, conf=conf)
        self.__selfID = selfAddress
        self.__autoUnlockTime = autoUnlockTime
        self.__mainThread = threading.current_thread()
        self.__initialised = threading.Event()
        self.__thread = threading.Thread(target=Lock._autoAcquireThread, args=(weakref.proxy(self),))
        self.__thread.start()
        while not self.__initialised.is_set():
            pass

    def _autoAcquireThread(self):
        print(f"{threading.get_ident()} _autoAcquireThread")
        self.__initialised.set()
        try:
            while True:
                if not self.__mainThread.is_alive():
                    break
                time.sleep(float(self.__autoUnlockTime) / 4.0)
                if self.__lockImpl._getLeader() is not None:
                    self.__lockImpl.ping(self.__selfID, time.time())
        except ReferenceError:
            pass

    def tryAcquireLock(self, path):
        self.__lockImpl.acquire(path, self.__selfID, time.time())

    def isAcquired(self, path):
        return self.__lockImpl.isAcquired(path, self.__selfID, time.time())

    def isOwned(self, path):
        return self.__lockImpl.isOwned(path, self.__selfID, time.time())

    def release(self, path):
        self.__lockImpl.release(path, self.__selfID)

    def getStatus(self):
        return self.__lockImpl.getStatus()

    def toggle_verbose(self):
        self.__lockImpl.toggle_verbose()

    def onTick(self):
        self.__lockImpl._onTick(timeToWait=0)


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world")


class StatusHandler(tornado.web.RequestHandler):
    sync_lock = None

    def initialize(self, sync_lock):
        self.sync_lock = sync_lock

    def get(self):
        self.write(str(self.sync_lock.getStatus()))


class ToggleHandler(tornado.web.RequestHandler):
    sync_lock = None

    def initialize(self, sync_lock):
        self.sync_lock = sync_lock

    def get(self):
        if self.sync_lock.isOwned("/dog"):
            self.sync_lock.release("/dog")
        else:
            self.sync_lock.tryAcquireLock("/dog")
        self.write("toggled") #str(self.sync_lock.isOwned("/dog")))


def make_app(sync_lock):
    return tornado.web.Application([
        (r"/", MainHandler),
        (r"/status", StatusHandler, {'sync_lock': sync_lock}),
        (r"/toggle", ToggleHandler, {'sync_lock': sync_lock}),
    ])


async def main():
    while main_control.thread_continue:
        await asyncio.sleep(1)

print(f"{threading.get_ident()} main")

selfAddr = "localhost:10000"
partners = ['localhost:10001', 'localhost:10002']

conf = SyncObjConf(autoTick=True)

sync_lock = Lock(selfAddr, partners, 10.0, conf=conf)

app = make_app(sync_lock)
web_port = int(selfAddr.split(":")[1]) + 1000
print(f"port: {web_port}")
while main_control.thread_continue:
    try:
        app.listen(web_port)
        break
    except Exception as e:
        print(e)
        time.sleep(1)

print('here again')

loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(main())
finally:
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()
