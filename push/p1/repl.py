import asyncio
import traceback
import uuid

import websockets
import time
import json
import pandas as pd
import numpy as np
import sys
import tblib.pickling_support

from code import compile_source

import pickle
import sys
from six import reraise
import dill
import code

tblib.pickling_support.install()


async def process_command(websocket, p, expect_response=True):
    tx = time.time_ns()
    cmd = p[0]
    if cmd == "c":
        message = f"{cmd};".encode("utf-8")
        message += dill.dumps(compile_source(p[1]))
    else:
        message = ';'.join(p)
    await websocket.send(message)
    tx2 = time.time_ns()
    s = await websocket.recv() if expect_response else None
    now = time.time_ns()
    return s, (now - tx) / 1000000, (now - tx2) / 1000000


def process_response(s):
    if s is None:
        return None
    ds = pickle.loads(s)
    if isinstance(ds, tuple) and issubclass(ds[0], BaseException):
        reraise(*ds)
    else:
        return ds


async def hello_cmd(websocket, line):
    p = line.split(" ")
    expect_response = True
    if p[0] == '=':
        p = [p[0], " ".join(p[1:])]
        expect_response = False
    elif p[0] == 'c':
        p = [p[0], " ".join(p[1:])]
    elif p[0].startswith("!"):
        x = line[1:]
        p = ['l', f"os.system(\"{x}\")"]
    else:
        p = ['l', " ".join(p)]
    try:
        s, tx, tx2 = await process_command(websocket, p, expect_response=expect_response)
        print(tx, tx2, np.round(tx - tx2, 4))
        print(process_response(s))
    except Exception as e:
        traceback.print_exc()
        print(e)
        pass


async def hello(host):
    async with websockets.connect(f'ws://{host}:8765', max_size=1024 * 1024 * 16) as websocket:
        print(f"{host} >>> ", end='')
        sys.stdout.flush()
        for line in sys.stdin:
            line = line.rstrip()
            await hello_cmd(websocket, line)
            print(f"{host} >>> ", end='')
            sys.stdout.flush()
    print()
    print(f"leaving {host}")


async def sac_cmd(line):
    if line.startswith("@"):
        host = line[1:]
        await hello(host)


async def sac():
    print(">>> ", end='')
    sys.stdout.flush()
    for line in sys.stdin:
        line = line.rstrip()
        await sac_cmd(line)
        print(">>> ", end='')
        sys.stdout.flush()


async def entry():
    cmd = None if len(sys.argv) == 1 else " ".join(sys.argv[1:])
    if cmd is not None:
        await sac_cmd(cmd)
    else:
        await sac()


asyncio.get_event_loop().run_until_complete(entry())
