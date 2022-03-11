from __future__ import print_function

import io
import json
import os
import sys
import types
import typing
import uuid
import zipfile
from importlib.abc import Loader as _Loader, MetaPathFinder as _MetaPathFinder
from importlib.machinery import ModuleSpec
from json import JSONDecodeError
from types import ModuleType

import dill
import requests


# https://docs.python.org/3/library/zipapp.html
# python -m zipapp d -m 'lockd:main'
# or if there's already a __main__.py then just
# python -m zipapp d

# add support for kvstore module - call main?
# https://stackoverflow.com/questions/1830727/how-to-load-compiled-python-modules-from-memory
# import importlib
# mod = imp.new_module('ciao')
# sys.modules['ciao'] = mod
# exec(s, mod.__dict__)



# helper to make creating package trees from flat package names:
# packages_to_dict({'a.I': 1, 'a.m.A': 2, 'a.m.M': 3})
# -->
# {'a': {'I': 1}, 'm': {'A': 2, 'M': 3}}
def packages_to_dict(pmap):
    d = {}
    for p, v in pmap.items():
        parts = p.split(".")
        s = list(reversed(parts[:-1]))
        q = d
        while len(s) > 0:
            m = s.pop()
            r = q.get(m)
            if r is None:
                r = {}
                q[m] = r
            q = r
        q[parts[-1]] = v
    return d


def ensure_path(p):
    import pathlib
    pathlib.Path(p).mkdir(parents=True, exist_ok=True)


def compile_file_path(m):
    with open(m, "r") as f:
        x = f.read()
    co = compile(x, str(uuid.uuid4()), "exec")
    return co


def compile_file_uri(m):
    # TODO: use proper URI parser
    return compile_file_path(m[len("file://"):])


def compile_uri(u):
    f = requests.get(u)
    x = f.text
    co = compile(x, str(uuid.uuid4()), "exec")
    return co


# TODO: use proper URI parser
def compile_source(t):
    if t.startswith("file://"):
        return compile_file_uri(t)
    elif t.startswith("http"):
        return compile_uri(t)
    return compile(t, str(uuid.uuid4()), "exec")


def load_url(u):
    return requests.get(u)


def load_url_data(u):
    return load_url(u).content


def load_url_text(u):
    f = load_url(u)
    try:
        return json.loads(f.text)
    except JSONDecodeError:
        return f.text


class ValueLoader(_Loader):

    def __init__(self, fullname, name, value):
        self.fullname = fullname
        self.name = name
        self.value = value

    def create_module(self, spec):
        parts = self.fullname.split(".")
        module = types.ModuleType(spec.name)
        module.__dict__['__push__'] = True
        module.__loader__ = self
        module.__package__ = parts[0]
        module.__file__ = spec.name
        module.__path__ = parts
        return module

    def apply_value(self, module, k, v):
        if isinstance(v, bytes):
            module.__dict__[k] = dill.loads(v)
        elif isinstance(v, types.CodeType):
            exec(v, module.__dict__)
        elif isinstance(v, str):
            v = compile(v, self.name, 'exec')
            exec(v, module.__dict__)
        elif isinstance(v, dict):
            sub_mod_fullname = f"{self.fullname}.{k}"
            dl = ValueLoader(sub_mod_fullname, k, v)
            spec = ModuleSpec(sub_mod_fullname, dl)
            m = dl.create_module(spec)
            dl.exec_module(m)
            module.__dict__[self.name] = m

    # everything needs to be in exec_module so reloading works
    def exec_module(self, module):
        try:
            if isinstance(self.value, dict):
                for k, v in self.value.items():
                    self.apply_value(module, k, v)
            else:
                self.apply_value(module, self.name, self.value)
        except ImportError as e:
            raise e
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"failed to load: {e}")
            raise ImportError(f"unable to import {module.__name__}")


class DictFinder(_MetaPathFinder):

    def __init__(self, store):
        self.store = store
        self.cache_store = None
        self.__build_key_cache()

    def find_module(self, fullname, path):
        return self.find_spec(fullname, path)

    def __build_key_cache(self):
        self.cache_store = packages_to_dict(self.store)
        show_dict(self.cache_store)

    def find_spec(self, fullname, path, target=None):
        parts = fullname.split(".")
        if len(parts) > 0 and parts[0] in self.cache_store:
            d = self.cache_store[parts[0]]
            s = list(reversed(parts[1:]))
            k = parts[0]
            while len(s) > 0:
                k = s.pop()
                if k not in d:
                    return None
                d = d[k]
                if not isinstance(d, dict):
                    break
            loader = ValueLoader(fullname, k, d) if isinstance(d, dict) else ValueLoader(fullname, k, d)
            return ModuleSpec(fullname, loader)

        return None

    def invalidate_caches(self):
        print(f"DictFinder: invalidating cache")
        self.__build_key_cache()


def load_module_pyz_loader(pyz_dict, name=None):
    name = name or str(uuid.uuid4())
    module = ModuleType(name)
    # pyz_dict = {k: zip_ref.read(k) for k in zip_ref.namelist()}
    finder = DictFinder(pyz_dict)
    sys.meta_path.insert(0, finder)
    try:
        exec(pyz_dict['__main__.py'], module.__dict__)
    finally:
        sys.meta_path.remove(finder)
    return module


def pyz_to_dict(m):
    with zipfile.ZipFile(m, "r") as zip_ref:
        return {k: zip_ref.read(k) for k in zip_ref.namelist()}


def load_module_pyz_file(m, name=None):
    return load_module_pyz_loader(pyz_to_dict(m), name)


def load_module_uri(m):
    if m.startswith("file://"):
        trim_file = m[len("file://") + 1:]
        return load_module(trim_file)
    elif m.startswith("http://") or m.startswith("https://"):
        # TODO: add whitelist for domains, etc. to minimize abuse
        # TODO: validate pyz data
        return load_module_pyz_loader(pyz_to_dict(m), io.BytesIO(load_url_data(m)))


# convert a directory structure of py modules into a dictionary for loading
# a
#   b
#     c
#       c1.py
#       c2.py
#       c3.py
#       d
#          d1.py
#          d2.py
#
# translates to
#
# {
#   'c': {
#     'c1.py': <c1.py>
#     'c2.py': <c2.py>
#     'c3.py': <c3.py>
#     'd': {
#       'd1': <d1.py>,
#       'd2': <d1.py>
#     }
#   }
# }
def dir_to_dict(root_dir):
    d = {}
    s: typing.List[typing.Dict] = [d]
    last_depth = len((os.path.normpath(root_dir)).split(os.path.sep))

    for root, directories, filenames in os.walk(root_dir):
        norm_path = os.path.normpath(root)
        path_parts = norm_path.split(os.path.sep)
        depth = len(path_parts)
        if depth < last_depth:
            s.pop()
        last_depth = depth
        parent = s[-1]
        child = {}
        child_name = os.path.splitext(path_parts[-1])[0]
        parent[child_name] = child
        if len(directories) > 0:
            s.append(child)
        for filename in filenames:
            if filename.endswith(".py"):
                with open(os.path.join(root, filename), "r") as f:
                    child_item_name = os.path.splitext(filename)[0]
                    child[child_item_name] = f.read()

    return d


def show_dict(d, level=0):
    indent = ' ' * 2 * level
    for k, v in d.items():
        if isinstance(v, dict):
            if k.startswith(os.path.sep):
                print(f'{indent}{k}')
            else:
                print(f'{indent}{os.path.sep}{k}')
            show_dict(v, level=level+1)
        else:
            print(f'{indent}{k}')


def load_module_dir(m, name=None):
    name = name or str(uuid.uuid4())
    sys.path.insert(0, m)
    try:
        module = ModuleType(name)
        with open(os.path.join(m, "__main__.py"), "r") as f:
            d = f.read()
        try:
            exec(d, module.__dict__)
        except Exception as e:
            print(f"FAILURE: {e}")
            raise e
    finally:
        sys.path.pop(0)
    return module


def load_module_py(m, name=None):
    name = name or str(uuid.uuid4())
    module = ModuleType(name)
    with open(m, "r") as f:
        exec(f.read(), module.__dict__)
    return module


# TODO: make relative imports work
# https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
# https://stackoverflow.com/questions/19850143/how-to-compile-a-string-of-python-code-into-a-module-whose-functions-can-be-call
def load_module(m):
    if isinstance(m, str):
        # TODO: move to provider / handler plugin model so that these handlers are not baked into the core
        if m.startswith("file://") or m.startswith("http://"):
            return load_module_uri(m)
        elif os.path.exists(m):
            if os.path.isdir(m):
                return load_module_dir(m)
            elif m.endswith(".pyz"):
                return load_module_pyz_file(m)
            elif m.endswith(".py"):
                return load_module_py(m)
    return None


def create_in_memory_module(name=None):
    # https://stackoverflow.com/questions/1830727/how-to-load-compiled-python-modules-from-memory
    import types
    name = name or str(uuid.uuid4())
    mod = types.ModuleType(name)
    sys.modules[name] = mod
    exec("", mod.__dict__)
    return mod


def load_in_memory_module(src, name=None):
    # https://stackoverflow.com/questions/1830727/how-to-load-compiled-python-modules-from-memory
    import types
    name = name or str(uuid.uuid4())
    mod = types.ModuleType(name)
    sys.modules[name] = mod
    s = src if isinstance(src, types.CodeType) else compile_source(src)
    exec(s, mod.__dict__)
    return mod, s


def load_src(kvstore, src):
    if isinstance(src, str):
        src = kvstore.get(src)
        if src is None:
            return None
    if isinstance(src, bytes):
        src = dill.loads(src)
    return src


def load_lambda(kvstore, src):
    src = load_src(kvstore, src)
    if isinstance(src, type):
        try:
            src = src()
            src = src.apply if hasattr(src, 'apply') else src
        except Exception as e:
            print(e)
            raise e
    return src


class KvStoreLambda:
    key: str

    def __init__(self, kvstore, key):
        self.kvstore = kvstore
        self.key = key

    def __call__(self, *args, **kwargs):
        self.apply(*args, **kwargs)

    def apply(self, *args, **kwargs):
        src = load_src(self.kvstore, self.key)
        if src is not None:
            try:
                src(*args, **kwargs)
            except Exception as e:
                print(e)

# useful helpers:
# https://stackoverflow.com/questions/1830727/how-to-load-compiled-python-modules-from-memory
# https://realpython.com/python-import/#finders-and-loaders
# https://bayesianbrad.github.io/posts/2017_loader-finder-python.html
# https://realpython.com/python-import/
# https://stackoverflow.com/questions/43571737/how-to-implement-an-import-hook-that-can-modify-the-source-code-on-the-fly-using
class CodeStoreLoader:

    # @staticmethod
    # def load_file(store, key_prefix, path):
    #     pass
    #
    # # @staticmethod
    # # def load_uri():
    #
    # # handles:
    # #   file: (file or dir), http:, github:<user>/<repo>
    # @staticmethod
    # def load(store, uri):
    #     pass
    #
    # @staticmethod
    # def export_dir(store, path, version=None):
    #     pass
    #
    # @staticmethod
    # def load_dir(store, path):
    #     pass

    @staticmethod
    def install(stores, enable_debug=False):
        import sys

        class DebugFinder(_MetaPathFinder):
            @classmethod
            def find_spec(cls, name, path, target=None):
                print(f"Importing {name!r}")
                return None

        finder = DictFinder(stores)

        sys.meta_path.insert(0, finder)
        if enable_debug:
            sys.meta_path.insert(0, DebugFinder())

        return finder
