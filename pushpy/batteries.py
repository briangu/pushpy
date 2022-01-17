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
from pysyncobj import replicated, SyncObjConsumer
from pysyncobj.batteries import ReplDict


class ReplEventDict(ReplDict):

    def __init__(self, on_set=None):
        self.on_set = on_set
        super(ReplEventDict, self).__init__()

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

    def __init__(self, on_head_change=None):
        self.on_head_change = on_head_change
        super(ReplVersionedDict, self).__init__()
        self.__objects = {}
        self.__references = {}
        self.__version = None
        self.__head = None
        self.__len_cache = {}

    def clear(self):
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
                # raise RuntimeError("no prior transactions")
                return
        else:
            if version is None:
                version = self.__version
            self.__head = min(version, self.__version)
            if self.on_head_change is not None:
                self.on_head_change(self.__head)

    def get_head(self):
        return self.__version if self.__head is None else self.__head

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
            q = module.__name__[len(self.scope) + 1:]
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
    def install_importer(stores, enable_debug=True):
        import sys

        class DebugFinder(_MetaPathFinder):
            @classmethod
            def find_spec(cls, name, path, target=None):
                print(f"Importing {name!r}")
                return None

        finder = PushFinder(stores)

        sys.meta_path.insert(0, finder)
        if enable_debug:
            sys.meta_path.insert(0, DebugFinder())

        return finder


class ReplTaskManager(SyncObjConsumer):

    def __init__(self, kvstore, task_manager):
        self.kvstore = kvstore
        self.task_manager = task_manager
        super(ReplTaskManager, self).__init__()

    # TODO: we should provide another way to store results as replicated actions would all store into the same key
    @replicated
    def apply(self, src, *args, **kwargs):
        ctx = self.kvstore.rawData().copy()
        return self.task_manager.apply(src, *args, ctx=ctx, **kwargs)


# Similar to _ReplLockManagerImpl but supports data bound to the lock
# TODO: can this be done with a lock and the dict?
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
