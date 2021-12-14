import sys
import uuid

import websocket
import threading, time

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
from websocket import ABNF

tblib.pickling_support.install()
import pickle, sys
from six import reraise
import dill
from code_util import compile_source


# https://newbedev.com/converting-a-python-function-with-a-callback-to-an-asyncio-awaitable


def make_iter():
    loop = asyncio.get_event_loop()
    queue = asyncio.Queue()

    def put(*args):
        loop.call_soon_threadsafe(queue.put_nowait, *args)

    async def get():
        return await queue.get()

    return get, put


def process_response(s):
    if s is None:
        return None
    ds = pickle.loads(s)
    if isinstance(ds, tuple) and issubclass(ds[0], BaseException):
        reraise(*ds)
    else:
        return ds


# https://github.com/websocket-client/websocket-client/issues/580

class NodeWebsocketClient(websocket.WebSocketApp):
    def __init__(self, *args, msg_queue, **kwargs):
        super().__init__(*args,
                         on_open=self.on_open,
                         on_close=self.on_close,
                         on_message=self.on_message,
                         on_error=self.on_error,
                         **kwargs)
        self.session_id = uuid.uuid4()
        self.context = globals().copy()
        self.cmd = None
        self.args = None
        self.done = False
        self.msg_queue = msg_queue

    def on_close(self, ws, a, b):
        # print('disconnected from server')
        print(f"{self.session_id} closed : %s" % time.ctime())
        sys.stdout.flush()
        # time.sleep(1)
        # connect_websocket()  # retry per 10 seconds

    def on_open(self, _):
        # self.send("l; 2+2")
        print(f'{self.session_id} connection established')

    def on_error(self, x, y):
        print(f"{self.session_id} error: {x} {y}")

    def on_message(self, ws, msg):
        self.msg_queue(process_response(msg))

    def retry(self):
        def x():
            while not self.done:
                try:
                    print(f"running")
                    self.run_forever()
                except Exception as e:
                    print(f"connection failed, will retry")
                    import traceback
                    traceback.print_exc()
                    print(e)
                time.sleep(1)
                print("Retry : %s" % time.ctime())
                sys.stdout.flush()

        return x

    def close(self, **kwargs):
        self.done = True
        super().close(**kwargs)


class NodeTask:
    ws: NodeWebsocketClient
    wst: threading.Thread

    def __init__(self, host):
        host = host or "127.0.0.1"
        self.msg_get, self.msg_put = make_iter()
        self.ws = NodeWebsocketClient(f"ws://{host}:8765", msg_queue=self.msg_put)
        self.wst = threading.Thread(target=self.ws.retry())
        self.wst.daemon = True

    def start(self):
        self.wst.start()

    def join(self, timeout=None):
        self.ws.close()
        self.wst.join(timeout=timeout)
        self.ws = None
        self.wst = None


async def hello_cmd(task, line):
    line = line.strip()
    if len(line) == 0:
        return
    p = line.split(" ")
    expect_response = True
    message = None
    opcode = ABNF.OPCODE_TEXT
    if p[0] == '=':
        p = [p[0], " ".join(p[1:])]
        expect_response = False
    elif p[0] == 'c':
        p = [p[0], " ".join(p[1:])]
        message = f"c;".encode("utf-8")
        message += dill.dumps(compile_source(p[1]))
        opcode = ABNF.OPCODE_BINARY
    elif p[0].startswith("!"):
        x = line[1:]
        p = ['l', f"os.system(\"{x}\")"]
    else:
        p = ['l', " ".join(p)]
    try:
        message = message or ';'.join(p)
        task.ws.send(message, opcode=opcode)
    except Exception as e:
        traceback.print_exc()
        print(e)
    return None if not expect_response else await task.msg_get()


async def hello(host):
    task = NodeTask(host)
    task.start()
    # async with websockets.connect(f'ws://{host}:8765', max_size=1024 * 1024 * 16) as websocket:
    print(f"{host} >>> ", end='')
    sys.stdout.flush()
    for line in sys.stdin:
        res = await hello_cmd(task, line)
        if res is not None:
            print(res)
        print(f"{host} >>> ", end='')
        sys.stdout.flush()
    print()
    print(f"leaving {host}")
    task.join(timeout=10)


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


if __name__ == "__main__":
    try:
        asyncio.get_event_loop().run_until_complete(entry())
    except Exception as err:
        print(err)
        print("connect failed")
