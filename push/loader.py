import zipfile
from types import ModuleType


# python -m zipapp d -m 'lockd:main'
def load_pyz(m, *args, **kwargs):
    module = ModuleType(m)
    context = module.__dict__.copy()
    with zipfile.ZipFile(m, "r") as zip_ref:
        # zip_ref.extractall('/tmp/process')
        for n in zip_ref.namelist():
            if n == '__main__.py':
                continue
            exec(zip_ref.read(n), context)
    return context


def load_py(m, *args, **kwargs):
    module = ModuleType(m)
    context = module.__dict__.copy()
    with open(m, "r") as f:
        exec(f.read(), context)
    return context


# TODO: make relative imports work
# https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
# https://stackoverflow.com/questions/19850143/how-to-compile-a-string-of-python-code-into-a-module-whose-functions-can-be-call
def load(m, *args, **kwargs):
    if isinstance(m, str):
        if m.endswith(".pyz"):
            return load_pyz(m, *args, **kwargs)
        elif m.endswith(".py"):
            return load_py(m, *args, **kwargs)


def load_and_run(m, *args, **kwargs):
    context = load(m, *args, **kwargs)
    # this will use the context provided in exec
    context['main'](*args, **kwargs)
