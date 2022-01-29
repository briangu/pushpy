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


class DictLoader(_Loader):

    def __init__(self, filename, fullname, data):
        self.filename = filename
        self.fullname = fullname
        self.data = data

    def create_module(self, spec):
        mod = types.ModuleType(spec.name)
        mod.__loader__ = self
        mod.__package__ = spec.name
        mod.__file__ = self.filename
        mod.__path__ = [self.filename]
        return mod

    def exec_module(self, module):
        try:
            module.__dict__[module.__name__] = self.filename
            exec(self.data, module.__dict__)
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"failed to load: {e}")


class DictFinder(_MetaPathFinder):

    def __init__(self, mod_dict):
        self.mod_dict = mod_dict

    def find_module(self, fullname, path):
        return self.find_spec(fullname, path)

    def find_spec(self, fullname, path, target=None):
        # py_name = f"{fullname}.py"
        if fullname in self.mod_dict:
            print(fullname, path)
            return ModuleSpec(fullname, DictLoader(fullname, fullname, self.mod_dict[fullname]))
        return None


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


# def load_module_pyz_inplace(m, name=None):
#     import os
#     name = name or str(uuid.uuid4())
#     module = ModuleType(name)
#     tmp_modules = sys.modules.copy()
#     try:
#         attempted = set()
#         with zipfile.ZipFile(m, "r") as zip_ref:
#             order = list(zip_ref.namelist())
#             while len(order) > 0:
#                 n = order.pop()
#                 if n == '__main__.py':
#                     if len(order) >= 1:
#                         order.insert(0, n)
#                     else:
#                         exec(zip_ref.read(n), module.__dict__)
#                 else:
#                     try:
#                         sub_module_name = os.path.splitext(n)[0]
#                         sub_module = ModuleType(sub_module_name)
#                         exec(zip_ref.read(n), sub_module.__dict__)
#                         sys.modules[sub_module_name] = sub_module
#                     except Exception as e:
#                         if n not in attempted:
#                             attempted.add(n)
#                             order.insert(0, n)
#                         else:
#                             print(e)
#                             raise
#     finally:
#         sys.modules = tmp_modules
#     return module
#
#
# def load_module_pyz(tmp_path, m, name=None):
#     name = name or str(uuid.uuid4())
#     tmp_path = os.path.join(tmp_path, 'pyz', str(name))
#     ensure_path(tmp_path)
#     sys.path.insert(0, tmp_path)
#     try:
#         module = ModuleType(name)
#         with zipfile.ZipFile(m, "r") as zip_ref:
#             zip_ref.extractall(tmp_path)
#             # TODO: call load_dir with extracted zip
#             try:
#                 exec(zip_ref.read("__main__.py"), module.__dict__)
#             except Exception as e:
#                 print(e)
#                 raise e
#     finally:
#         sys.path.pop(0)
#     return module


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
    indent = ' ' * 4 * level
    for k, v in d.items():
        if isinstance(v, dict):
            print('{}{}{}'.format(indent, os.path.sep, k))
            show_dict(v, level=level+1)
        else:
            print('{}{}'.format(indent, k))


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


def load_module_and_run(m, log, *args, **kwargs):
    if not os.path.exists(m):
        orig_m = m
        m = os.path.join(os.path.dirname(__file__), m)
        log.warn(f"{orig_m} not found, using current dir for {m}")
        if not os.path.exists(m):
            log.error(f"module not found: {m}")
            raise RuntimeError(f"module not found: {m}")
    log.info(f"loading and running: {m}")
    module = load_module(m)
    if 'main' not in module.__dict__:
        log.error(f"missing main function in module: {m}")
        raise RuntimeError(f"missing main function in module: {m}")
    module.__dict__['main'](*args, **kwargs)


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
        if src.startswith("kvstore:"):
            p = src.split(":")
            src = kvstore.get(p[1])
            if src is None:
                return None
            src = dill.loads(src)
        else:
            # TODO: should we attempt dill.loads here?
            # TODO: what do we with do a URI src?
            # src = compile_source(src)
            return None
    elif isinstance(src, bytes):
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
        self.key = f"kvstore:{key}"

    def __call__(self, *args, **kwargs):
        self.apply(*args, **kwargs)

    def apply(self, *args, **kwargs):
        src = load_src(self.kvstore, self.key)
        if src is not None:
            try:
                src(*args, **kwargs)
            except Exception as e:
                print(e)


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
    def install_finder(stores, enable_debug=True):
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
