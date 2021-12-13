import os
import sys
import uuid
import zipfile
from types import ModuleType


# python -m zipapp d -m 'lockd:main'
# or if there's already a __main__.py then just
# python -m zipapp d

# def load_pyz(m):
#     module = ModuleType('lock')
#     context = module.__dict__.copy()
#     with zipfile.ZipFile(m, "r") as zip_ref:
#         # zip_ref.extractall('/tmp/process')
#         order = list(zip_ref.namelist())
#         print(order)
#         while len(order) > 0:
#             n = order.pop()
#             print(n)
#             if n == '__main__.py':
#                 continue
#             try:
#                 print(context.keys())
#                 exec(zip_ref.read(n), context)
#             except Exception as e:
#                 print(e)
#                 order.insert(0, n)
#     return context

def ensure_path(p):
    import pathlib
    pathlib.Path(p).mkdir(parents=True, exist_ok=True)


def load_dir(tmp_path, m):
    tmp_id = str(uuid.uuid4())
    # print(tmp_path)
    # tmp_path = os.path.join(tmp_path, 'pyz', str(tmp_id))
    # ensure_path(tmp_path)
    sys.path.insert(0, m)
    module = ModuleType(tmp_id)
    context = module.__dict__.copy()
    with open(os.path.join(m, "__main__.py"), "r") as f:
        d = f.read()
    try:
        exec(d, context)
    except Exception as e:
        print(f"FAILURE: {e}")
        raise e
    sys.path.pop(0)
    return context


def load_pyz(tmp_path, m):
    tmp_id = str(uuid.uuid4())
    tmp_path = os.path.join(tmp_path, 'pyz', str(tmp_id))
    ensure_path(tmp_path)
    sys.path.insert(0, tmp_path)
    module = ModuleType(tmp_id)
    context = module.__dict__.copy()
    with zipfile.ZipFile(m, "r") as zip_ref:
        zip_ref.extractall(tmp_path)
        # TODO: call load_dir with extracted zip
        try:
            exec(zip_ref.read("__main__.py"), context)
        except Exception as e:
            print(e)
            raise e
    sys.path.pop(0)
    return context


def load_py(m):
    module = ModuleType(m)
    context = module.__dict__.copy()
    with open(m, "r") as f:
        exec(f.read(), context)
    return context


# TODO: make relative imports work
# https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
# https://stackoverflow.com/questions/19850143/how-to-compile-a-string-of-python-code-into-a-module-whose-functions-can-be-call
def load(tmp_path, m):
    print(m)
    if os.path.isdir(m):
        return load_dir(tmp_path, m)
    elif isinstance(m, str):
        if m.endswith(".pyz"):
            return load_pyz(tmp_path, m)
        elif m.endswith(".py"):
            return load_py(m)


def load_and_run(tmp_path, m, log, *args, **kwargs):
    if not os.path.exists(m):
        orig_m = m
        m = os.path.join(os.path.dirname(__file__), m)
        log.warn(f"{orig_m} not found, using current dir for {m}")
        if not os.path.exists(m):
            log.error(f"module not found: {m}")
            raise RuntimeError(f"module not found: {m}")
    log.info(f"loading and running: {m}")
    context = load(tmp_path, m)
    print(context['main'])
    # this will use the context provided in exec
    if 'main' not in context:
        log.error(f"missing main function in module: {m}")
        raise RuntimeError(f"missing main function in module: {m}")
    context['main'](*args, **kwargs)
