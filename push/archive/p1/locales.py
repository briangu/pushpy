import asyncio

import websockets
import time
import sys
import random

from archive.lambda_client import process_command, process_response

import tblib.pickling_support
tblib.pickling_support.install()

host = sys.argv[1]


# cmd = sys.argv[1]
# cmd_args = sys.argv[2:]
# print(cmd, cmd_args)

class Locale:

    def __init__(self):
        self.x = None

    async def __aenter__(self):
        self.x = [websockets.connect(f'ws://{host}:876{5 + i}', max_size=1024 * 1024 * 16) for i in range(2)]
        return await asyncio.gather(*[w.__aenter__() for w in self.x])

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        ws = self.x
        self.x = None
        return await asyncio.gather(*[w.__aexit__(exc_type, exc_val, exc_tb) for w in ws])


async def locale_test():
    async with Locale() as locales:
        locale = random.choice(locales)
        tx = time.time_ns()
        s, tx2 = await process_command(locale)
        tx = (time.time_ns() - tx) / 1000000
        print(tx, 1000 / tx, tx2, 1000 / tx2)
        print(process_response(s))


asyncio.get_event_loop().run_until_complete(locale_test())
