import os
import sys
import uuid
import zipfile
from types import ModuleType


# python -m zipapp d -m 'lockd:main'
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


def load_pyz(m):
    tmp_path = f'/tmp/push/{uuid.uuid4()}'
    ensure_path(tmp_path)
    sys.path.insert(0, tmp_path)
    module = ModuleType('lock')
    context = module.__dict__.copy()
    with zipfile.ZipFile(m, "r") as zip_ref:
        zip_ref.extractall(tmp_path)
        exec(zip_ref.read("main.py"), context)
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
def load(m):
    print(m)
    if isinstance(m, str):
        if m.endswith(".pyz"):
            return load_pyz(m)
        elif m.endswith(".py"):
            return load_py(m)


def load_and_run(m, log, *args, **kwargs):
    if not os.path.exists(m):
        orig_m = m
        m = os.path.join(os.path.dirname(__file__), m)
        log.warn(f"{orig_m} not found, using current dir for {m}")
        if not os.path.exists(m):
            log.error(f"module not found: {m}")
            raise RuntimeError(f"module not found: {m}")
    log.info(f"loading and running: {m}")
    context = load(m)
    print(context['main'])
    # this will use the context provided in exec
    if 'main' not in context:
        log.error(f"missing main function in module: {m}")
        raise RuntimeError(f"missing main function in module: {m}")
    context['main'](*args, **kwargs)
