import os
import zipfile
from types import ModuleType


# python -m zipapp d -m 'lockd:main'
def load_pyz(m):
    module = ModuleType(m)
    context = module.__dict__.copy()
    with zipfile.ZipFile(m, "r") as zip_ref:
        # zip_ref.extractall('/tmp/process')
        for n in zip_ref.namelist():
            if n == '__main__.py':
                continue
            exec(zip_ref.read(n), context)
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
        m = os.path.join(os.path.dirname(__file__), m)
        log.warn(f"module not found, using current dir for {m}")
        if not os.path.exists(m):
            log.error(f"module not found: {m}")
            raise RuntimeError(f"module not found: {m}")
    log.info(f"loading and running: {m}")
    context = load(m)
    # this will use the context provided in exec
    if 'main' not in context:
        log.error(f"missing main function in module: {m}")
        raise RuntimeError(f"missing main function in module: {m}")
    context['main'](*args, **kwargs)
