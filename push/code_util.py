import uuid
import requests


def compile_file(n):
    with open(n, "r") as f:
        x = f.read()
    co = compile(x, str(uuid.uuid4()), "exec")
    return co


def compile_uri(u):
    f = requests.get(u)
    x = f.text
    co = compile(x, str(uuid.uuid4()), "exec")
    return co


def compile_source(t):
    if t.startswith("file://"):
        return compile_file(t[len("file://"):])
    elif t.startswith("http"):
        return compile_uri(t)
    return compile(t, str(uuid.uuid4()), "exec")


