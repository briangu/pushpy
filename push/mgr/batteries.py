from __future__ import print_function

import hashlib
import os
import socket
import threading
import time
import weakref

import dill
import numpy as np
import pandas as pd
from pysyncobj import replicated, SyncObjConsumer
from pysyncobj import replicated_sync
from pysyncobj.batteries import ReplDict

from push.mgr.code_util import load_src


class ReplSyncDict(ReplDict):

    def __init__(self, on_set=None):
        self.on_set = on_set
        super(ReplSyncDict, self).__init__()

    @replicated_sync
    def set_sync(self, key, value):
        self.set(key, value, _doApply=True)

    @replicated
    def set(self, key, value):
        super().set(key, value, _doApply=True)
        if self.on_set is not None:
            self.on_set(key, value)


# # TODO: scan repl_obj for all methods and add to this, proxying all replicated operations to parent
# class _ReplDynamicProxy:
#     def __init__(self, parent, name, repl_obj):
#         self.parent = parent
#         self.name = name
#         self.repl_obj = repl_obj
#
#     def apply(self, method, *args, **kwargs):
#         return self.parent.apply(self.name, method, *args, **kwargs)
#
#
# # usage: this will be the base repl data structure for a Push server
# #       it supports adding new / removing named sub-consumers as a replicated action
# #       it supports operating on added sub-consumers as a replicated action
# class ReplDynamicConsumer(SyncObjConsumer):
#     def __init__(self):
#         super(ReplDynamicConsumer, self).__init__()
#         self.__properties = set()
#         for key in self.__dict__:
#             self.__properties.add(key)
#         self.__data = {}
#
#     def obj_from_type(self, repl_type):
#         if repl_type == "list":
#             obj = ReplList()
#         elif repl_type == "dict":
#             obj = ReplDict()
#         elif repl_type == "ts":
#             obj = ReplTimeseries()
#         else:
#             raise RuntimeError(f"unknown type: {repl_type}")
#         obj._syncObj = self
#         return obj
#
#     @replicated
#     def add(self, name, repl_type):
#         if name in self.__data:
#             raise RuntimeError(f"name already present: {name}")
#         self.__data[name] = {'type': repl_type, 'obj': self.obj_from_type(repl_type)}
#
#     @replicated
#     def remove(self, name):
#         self.__delitem__(name, _doApply=True)
#
#     @replicated
#     def __delitem__(self, name):
#         if name in self.__data:
#             del self.__data[name]
#
#     @replicated
#     def apply(self, name, method, *args, **kwargs):
#         if name not in self.__data:
#             raise RuntimeError(f"name already present: {name}")
#         d = self.__data[name]['obj']
#         if not hasattr(d, method):
#             raise RuntimeError(f"method not found: {name} {method}")
#         return getattr(d, method)(*args, **kwargs)
#
#     def __getitem__(self, name):
#         repl_obj = self.__data.get(name)['obj']
#         return _ReplDynamicProxy(self, name, repl_obj) if repl_obj is not None else None
#
#     def _serialize(self):
#         d = dict()
#         for k, v in [(k, v) for k, v in iteritems(self.__dict__) if k not in self.__properties]:
#             if k.endswith("__data") and isinstance(v, dict):
#                 _d = dict()
#                 for _k, _v in iteritems(v):
#                     __d = dict()
#                     __d['type'] = _v['type']
#                     __d['obj'] = _v['obj']._serialize()
#                     _d[_k] = __d
#                 v = _d
#             d[k] = v
#         return d
#
#     # TODO: recurse into subconsumers
#     def _deserialize(self, data):
#         for k, v in iteritems(data):
#             if k.endswith("__data") and isinstance(v, dict):
#                 _d = dict()
#                 for _k, _v in iteritems(v):
#                     __d = dict()
#                     __d['type'] = _v['type']
#                     obj = self.obj_from_type(_v['type'])
#                     obj._deserialize(_v['obj'])
#                     __d['obj'] = obj
#                     _d[_k] = __d
#                 v = _d
#             self.__dict__[k] = v

#
# Replicated Code Store with versioning
#   ex usage:
#       obj.inc_version()
#       obj.set("/a", pickle.dumps(lambda: 1))
#       obj.set("/b", pickle.dumps(lambda: 2))
#       obj.commit()
#       obj.inc_version()
#       obj.set("/a", pickle.dumps(lambda: 3))
#       obj.commit()
#       v = obj.get("/a")
#       v() expect ==> 3
# TODO: add enumerate all keys /w lambda
#       flush to disk directory?
#       load from disk directory?
class ReplCodeStore(SyncObjConsumer):

    def __init__(self):
        super(ReplCodeStore, self).__init__()
        self.__objects = {}
        self.__references = {}
        self.__version = 0
        self.__head = None


    @staticmethod
    def hash_obj(value):
        m = hashlib.sha256()
        m.update(value)
        return m.digest()

    def store_obj(self, value):
        data = dill.dumps(value)
        key = self.hash_obj(data)
        self.__objects[key] = data
        return key

    def get_obj(self, key):
        obj = self.__objects.get(key)
        return dill.loads(obj) if obj is not None else None

    # TODO: specify requirements and dependent data keys (from data space)
    @replicated_sync
    def set_sync(self, key, value):
        self.set(key, value, _doApply=True)

    @replicated_sync
    def inc_version_sync(self):
        self.inc_version(_doApply=True)

    @replicated
    def inc_version(self):
        self.__version += 1
        return self.__version

    def get_version(self):
        return self.__version

    @replicated_sync
    def set_head_sync(self, version=None):
        self.set_head(version=version, _doApply=True)

    @replicated
    def set_head(self, version=None):
        version = version or self.__version
        self.__head = min(version, self.__version)

    def get_head(self):
        return self.__head

    @replicated_sync
    def commit_sync(self):
        self.commit(_doApply=True)

    @replicated
    def commit(self):
        self.set_head(version=None, _doApply=True)

    @staticmethod
    def floor_to_version(arr, version):
        for i in reversed(range(len(arr))):
            v = arr[i][0]
            if v <= version:
                return arr[i][1]
        return None

    def get(self, key, version=None):
        version = version or self.get_head()
        arr = self.__references.get(key)
        if arr is not None:
            v = self.floor_to_version(arr, version)
            if v is not None:
                return self.get_obj(v)
        return None

    @replicated_sync
    def add_sync(self, items):
        self.add(items, _doApply=True)

    @replicated
    def add(self, *args):
        self.inc_version(_doApply=True)
        items = args[0] if len(args) == 1 else [tuple(args[:2])]
        for key, value in items:
            self.set(key, value, _doApply=True)
        self.commit(_doApply=True)

    @replicated_sync
    def set_sync(self, key, value):
        self.set(key, value, _doApply=True)

    @replicated
    def set(self, key, value):
        obj_key = self.store_obj(value)
        arr = self.__references.get(key)
        if arr is None:
            arr = []
        arr.append((self.__version, obj_key))
        self.__references[key] = arr

    @replicated_sync
    def apply_sync(self, key, *args, **kwargs):
        return self.apply(key, *args, **kwargs, _doApply=True)

    # TODO: add ability to store result in (a?) kvstore
    # TODO: add import context - can we execute with a specified context?
    # TODO: this will replay on load.  is this the desired behavior?
    #           can we make it so that they don't rerun if they aren't new or too old?
    # NOTE: all operations need to be idempotent
    @replicated
    def apply(self, key, *args, **kwargs):
        key = key if key.startswith("kvstore:") else f"kvstore:{key}"
        src = load_lamdba(self, key)
        if src is not None:
            ctx = {'src': src, 'args': args, 'kwargs': kwargs}
            # TOOD: support lambda requirements
            # exec('import math', ctx)
            try:
                exec(f"r = src(*args, **kwargs)", ctx)
                print(ctx['r'])
                return ctx['r']
            except Exception as e:
                return e
        return None


class ReplTimeseries(SyncObjConsumer):
    def __init__(self, on_append=None):
        self.__on_append = on_append
        super(ReplTimeseries, self).__init__()
        self.__data = dict()
        self.__index_data = list()

    @replicated
    def reset(self):
        self.__data = dict()
        self.__index_data = list()

    @replicated
    def append(self, idx_data, keys, data):
        self.__index_data.append(idx_data)
        for key, key_data in zip(keys, data):
            col = self.__data.get(key)
            if col is None:
                col = list()
                self.__data[key] = col
            key_data = key_data if isinstance(key_data, list) else [key_data]
            col.append(key_data)
        if self.__on_append is not None:
            self.__on_append(idx_data, keys, data)

    def flatten(self, keys=None):
        keys = keys or list(self.__data.keys())
        df = pd.DataFrame(columns=keys, index=self.__index_data)
        for key in keys:
            df[key] = np.concatenate(self.__data[key])
        return df


class _ReplLockDataManagerImpl(SyncObjConsumer):
    def __init__(self, autoUnlockTime):
        super(_ReplLockDataManagerImpl, self).__init__()
        self.__locks = {}
        self.__autoUnlockTime = autoUnlockTime

    @replicated
    def acquire(self, lockID, clientID, currentTime, data=None):
        existingLock = self.__locks.get(lockID, None)
        # Auto-unlock old lock
        if existingLock is not None:
            if currentTime - existingLock[1] > self.__autoUnlockTime:
                existingLock = None
        # Acquire lock if possible
        if existingLock is None or existingLock[0] == clientID:
            self.__locks[lockID] = (clientID, currentTime, data)
            return True
        # Lock already acquired by someone else
        return False

    @replicated
    def prolongate(self, clientID, currentTime):
        for lockID in list(self.__locks):
            lockClientID, lockTime, lockData = self.__locks[lockID]

            if currentTime - lockTime > self.__autoUnlockTime:
                del self.__locks[lockID]
                continue

            if lockClientID == clientID:
                self.__locks[lockID] = (lockClientID, currentTime, lockData)

    @replicated
    def release(self, lockID, clientID):
        existingLock = self.__locks.get(lockID, None)
        if existingLock is not None and existingLock[0] == clientID:
            del self.__locks[lockID]

    def isAcquired(self, lockID, clientID, currentTime):
        existingLock = self.__locks.get(lockID, None)
        if existingLock is not None:
            if existingLock[0] == clientID:
                if currentTime - existingLock[1] < self.__autoUnlockTime:
                    return True
        return False

    def isOwned(self, lockID, currentTime):
        existingLock = self.__locks.get(lockID, None)
        if existingLock is not None:
            if currentTime - existingLock[1] < self.__autoUnlockTime:
                return True
        return False

    def lockData(self, lockID=None):
        if lockID is None:
            return {k: self.__locks[k][2] for k in self.__locks.keys()}
        existingLock = self.__locks.get(lockID)
        if existingLock is not None:
            return {lockID: existingLock}


class ReplLockDataManager(object):

    def __init__(self, autoUnlockTime, selfID = None):
        """Replicated Lock Manager. Allow to acquire / release distributed locks.

        :param autoUnlockTime: lock will be released automatically
            if no response from holder for more than autoUnlockTime seconds
        :type autoUnlockTime: float
        :param selfID: (optional) - unique id of current lock holder.
        :type selfID: str
        """
        self.__lockImpl = _ReplLockDataManagerImpl(autoUnlockTime)
        if selfID is None:
            selfID = '%s:%d:%d' % (socket.gethostname(), os.getpid(), id(self))
        self.__selfID = selfID
        self.__autoUnlockTime = autoUnlockTime
        self.__mainThread = threading.current_thread()
        self.__initialised = threading.Event()
        self.__destroying = False
        self.__lastProlongateTime = 0
        self.__thread = threading.Thread(target=ReplLockDataManager._autoAcquireThread, args=(weakref.proxy(self),))
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

    def tryAcquire(self, lockID, data=None, callback=None, sync=False, timeout=None):
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
            acquireRes = self.__lockImpl.acquire(lockID, self.__selfID, attemptTime, data=data, callback=callback, sync=sync, timeout=timeout)
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

    def isOwned(self, lockID):
        return self.__lockImpl.isOwned(lockID, time.time())

    def lockData(self, lockID=None):
        return self.__lockImpl.lockData(lockID=lockID)

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
