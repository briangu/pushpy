from __future__ import print_function

import hashlib
import os
import socket
import threading
import time
import types
import weakref
from collections import Mapping
from importlib.abc import Loader as _Loader, MetaPathFinder as _MetaPathFinder
from importlib.machinery import ModuleSpec
from typing import ValuesView, ItemsView

import dill
import numpy as np
import pandas as pd
from pysyncobj import replicated, SyncObjConsumer
from pysyncobj import replicated_sync
from pysyncobj.batteries import ReplDict


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

#   with obj:
#       obj.set("/a", pickle.dumps(lambda: 1))
#       obj.set("/b", pickle.dumps(lambda: 2))
#
# get:
#   obj['/']
#   obj.get("/")

# TODO: grab a lock for commit transaction otherwise a seperate process can
class ReplVersionedDict(SyncObjConsumer, Mapping):

    def __init__(self):
        super(ReplVersionedDict, self).__init__()
        self.__objects = {}
        self.__references = {}
        self.__version = None
        self.__head = None
        self.__len_cache = {}

    def __getitem__(self, k):
        x = self.get(k)
        if x is None:
            raise KeyError(k)
        return x

    def __len__(self):
        version = self.get_head()
        if version in self.__len_cache:
            return self.__len_cache[version]
        x = sum([1 for arr in self.__references.values() if self.__floor_to_version(arr, version) is not None])
        self.__len_cache[version] = x
        return x

    # https://docs.python.org/3/reference/datamodel.html#object.__iter__
    def __iter__(self):
        return self.keys()

    # TODO: create ItemsView
    def items(self) -> ItemsView:
        version = self.get_head()
        for key, arr in self.__references.items():
            v = self.__floor_to_version(arr, version)
            if v is not None:
                yield key, self.__get_obj(v)

    # TODO: create ValuesView
    def values(self) -> ValuesView:
        version = self.get_head()
        for key, arr in self.__references.items():
            v = self.__floor_to_version(arr, version)
            if v is not None:
                yield self.__get_obj(v)

    def __contains__(self, o: object) -> bool:
        return self.get(o) is not None

    @replicated
    def delete(self, key):
        self.__delitem__(key, _doApply=True)

    @replicated
    def __delitem__(self, key):
        # put a tombstone into the end of the array so that it's ignored in __floor_to_version
        self.set(key, None, _doApply=True)

    # https://stackoverflow.com/questions/42366856/keysview-valuesview-and-itemsview-default-representation-of-a-mapping-subclass
    # TODO: impelement KeysView so it works over BaseManager
    def keys(self, version=None):
        version = version or self.get_head()
        all_keys = []
        for key, arr in self.__references.items():
            v = self.__floor_to_version(arr, version)
            if v is not None:
                all_keys.append(key)
        return all_keys.__iter__()

    @staticmethod
    def __hash_obj(value):
        m = hashlib.sha256()
        m.update(value)
        return m.digest()

    def __store_obj(self, value):
        data = dill.dumps(value)
        key = self.__hash_obj(data)
        self.__objects[key] = data
        return key

    def __get_obj(self, key):
        obj = self.__objects.get(key)
        return dill.loads(obj) if obj is not None else None

    def __inc_version(self):
        self.__version = 0 if self.__version is None else self.__version + 1
        return self.__version

    def get_max_version(self):
        return self.__version

    @replicated
    def set_head(self, version=None):
        if self.__version is None:
            if version is not None:
                raise RuntimeError("no prior transactions")
        else:
            self.__head = min(version or self.__version, self.__version)

    def get_head(self):
        return self.__head or self.__version

    @replicated
    def update(self, other):
        self.__inc_version()
        for k in other:
            self.__set(k, other[k])
        self.set_head(version=None, _doApply=True)

    @staticmethod
    def __floor_to_version(arr, version):
        for i in reversed(range(len(arr))):
            v = arr[i][0]
            if v <= version:
                return arr[i][1]
        return None

    def get(self, key):
        version = self.get_head()
        arr = self.__references.get(key)
        if arr is not None:
            v = self.__floor_to_version(arr, version)
            if v is not None:
                return self.__get_obj(v)
        return None

    def __set(self, key, value):
        obj_key = self.__store_obj(value) if value is not None else None
        arr = self.__references.get(key)
        if arr is None:
            arr = []
        arr.append((self.__version, obj_key))
        self.__references[key] = arr

    @replicated
    def set(self, key, value):
        self.update({key: value}, _doApply=True)

    @replicated
    def flatten(self):
        pass


class PushLoader(_Loader):

    def __init__(self, scope, store):
        self.scope = scope
        self.store = store

    def create_module(self, spec):
        mod = types.ModuleType(spec.name)
        mod.__dict__['__push'] = True
        mod.__loader__ = self
        mod.__package__ = spec.name
        mod.__file__ = spec.name
        mod.__path__ = []
        return mod

    def exec_module(self, module):
        try:
            module.__dict__[module.__name__] = module
            q = module.__name__[len(self.scope)+1:]
            for key in self.store.keys():
                if key.startswith(q):
                    module.__dict__[key.split('.')[-1]] = dill.loads(self.store[key])
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"failed to load: {e}")


class PushFinder(_MetaPathFinder):

    def __init__(self, stores):
        self.stores = stores

    def find_module(self, fullname, path):
        return self.find_spec(fullname, path)

    def find_spec(self, fullname, path, target=None):
        p = fullname.split(".")
        if p[0] in self.stores:
            print(f"PushFinder:Importing {fullname!r}")
            return ModuleSpec(fullname, PushLoader(p[0], self.stores[p[0]]))
        return None


# useful helpers:
# https://stackoverflow.com/questions/1830727/how-to-load-compiled-python-modules-from-memory
# https://realpython.com/python-import/#finders-and-loaders
# https://bayesianbrad.github.io/posts/2017_loader-finder-python.html
# https://realpython.com/python-import/
# https://stackoverflow.com/questions/43571737/how-to-implement-an-import-hook-that-can-modify-the-source-code-on-the-fly-using
class CodeStoreLoader:

    @staticmethod
    def load_github(store, key_prefix, repo):
        from github import Github
        g = Github()
        repo = g.get_repo(repo)
        contents = repo.get_contents("")
        while contents:
            file_content = contents.pop(0)
            if file_content.type == "dir":
                contents.extend(repo.get_contents(file_content.path))
            else:
                print(file_content.path)
                # store.set(f"{key_prefix}file_content.path")

    @staticmethod
    def load_file(store, key_prefix, path):
        pass

    # @staticmethod
    # def load_uri():

    # handles:
    #   file: (file or dir), http:, github:<user>/<repo>
    @staticmethod
    def load(store, uri):
        pass

    @staticmethod
    def export_dir(store, path, version=None):
        pass

    @staticmethod
    def load_dir(store, path):
        pass

    @staticmethod
    def install_importer(stores):
        import sys

        class DebugFinder(_MetaPathFinder):
            @classmethod
            def find_spec(cls, name, path, target=None):
                print(f"Importing {name!r}")
                return None

        sys.meta_path.insert(0, PushFinder(stores))
        sys.meta_path.insert(0, DebugFinder())


class ReplTaskManager(SyncObjConsumer):

    def __init__(self, kvstore, task_manager):
        self.kvstore = kvstore
        self.task_manager = task_manager
        super(ReplTaskManager, self).__init__()

    # @replicated_sync
    # def apply_sync(self, src, *args, result_key=None, **kwargs):
    #     return self.apply(src, *args, result_key=result_key, **kwargs, _doApply=True)

    # TODO: we should provide another way to store results as replicated actions would all store into the same key
    @replicated
    def apply(self, src, *args, **kwargs):
        ctx = self.kvstore.rawData().copy()
        return self.task_manager.run("lambda", src, *args, ctx=ctx, **kwargs)


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

    def __init__(self, autoUnlockTime, selfID=None):
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
