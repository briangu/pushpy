import asyncio
import sys
import uuid

import dill

from push.push_manager import PushManager




# async def hello_cmd(dt, line):
#     p = line.split(" ")
#     expect_response = True
#     if p[0] == '=':
#         p = [p[0], " ".join(p[1:])]
#         expect_response = False
#     elif p[0] == 'c':
#         p = [p[0], " ".join(p[1:])]
#     elif p[0].startswith("!"):
#         x = line[1:]
#         p = ['l', f"os.system(\"{x}\")"]
#     else:
#         p = ['l', " ".join(p)]
#     try:
#         s, tx, tx2 = await process_command(websocket, p, expect_response=expect_response)
#         print(tx, tx2, np.round(tx - tx2, 4))
#         print(process_response(s))
#     except Exception as e:
#         traceback.print_exc()
#         print(e)
#         pass


async def hello(host):
    m = PushManager(address=('', int(host)), authkey=b'password')
    m.connect()
    dt = m.local_tasks()
    print(f"{host} >>> ", end='')
    sys.stdout.flush()
    for line in sys.stdin:
        cmd = line.rstrip()
        code = compile(cmd, str(uuid.uuid4()), "eval")
        r = dt.apply(dill.dumps(code))
        print(r)
        print(f"{host} >>> ", end='')
        sys.stdout.flush()
    print()
    print(f"leaving {host}")


async def sac_cmd(line):
    if line.startswith("@"):
        host = line[1:]
        await hello(host)
    elif line == "hosts":
        # TODO: connect to host and list list of hosts
        pass



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
