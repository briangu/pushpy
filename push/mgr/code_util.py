import dill


def load_src(kvstore, src):
    if isinstance(src, str):
        if src.startswith("kvstore:"):
            p = src.split(":")
            src = kvstore.get(p[1])
            if src is None:
                return None
            src = dill.loads(src)
        else:
            # TODO: what do we with do a URI src?
            # src = compile_source(src)
            return None
    else:
        src = dill.loads(src)
    if isinstance(src, type):
        src = src()
        src = src.apply if hasattr(src, 'apply') else src
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
            src(*args, **kwargs)


# https://stackoverflow.com/questions/1830727/how-to-load-compiled-python-modules-from-memory
# import importlib
# mod = imp.new_module('ciao')
# sys.modules['ciao'] = mod
# exec(s, mod.__dict__)
