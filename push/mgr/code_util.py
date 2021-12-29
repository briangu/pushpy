import dill


def load_src(kvstore, src):
    if isinstance(src, str):
        # print(f"load_src: {src}")
        p = src.split(":")
        src = kvstore.get(p[1])
        if src is None:
            return None
        src = dill.loads(src)
    else:
        # print(f"load_src: code")
        src = dill.loads(src)
    # print(f"load_src: {type(src)}")
    if isinstance(src, type):
        src = src()
        src = src.apply if hasattr(src, 'apply') else src
    return src


class KvStoreLambda:
    key: str

    def __init__(self, kvstore, key):
        self.kvstore = kvstore
        self.key = f"kvstore:{key}"

    def apply(self, *args, **kwargs):
        src = load_src(self.kvstore, self.key)
        if src is not None:
            src(*args, **kwargs)
