#!../venv/bin/python3

import asyncio
import sys
import uuid

import dill

from pushpy.push_manager import PushManager
from pushpy.push_server_utils import host_to_address

push_managers = dict()
default_host = sys.argv[1]


def connect_to_host(host):
    if host not in push_managers:
        m = PushManager(address=host_to_address(host), authkey=b'password')
        m.connect()
        push_managers[host] = m
    return push_managers[host]


def host_exec_cmd(dt, cmd):
    code = compile(cmd, str(uuid.uuid4()), "eval")
    return dt.apply(dill.dumps(code))


def hello_cmd(cmd):
    cmd = cmd[1:]
    if cmd == "host" or cmd == "host_resources":
        cmd = "host_resources"
    return cmd


async def hello(host):
    m = connect_to_host(host)
    dt = m.local_tasks()
    print(f"{host} >>> ", end='')
    sys.stdout.flush()
    for line in sys.stdin:
        cmd = line.rstrip()
        cmd = hello_cmd(cmd) if cmd.startswith("!") else cmd
        if len(cmd) > 0:
            r = host_exec_cmd(dt, cmd)
            print(r)
        print(f"{host} >>> ", end='')
        sys.stdout.flush()
    print()
    print(f"leaving {host}")


async def sac_cmd(cmd):
    if cmd.startswith("@"):
        host = cmd[1:]
        await hello(host)
    elif cmd == "hosts":
        m = connect_to_host(default_host)
        dt = m.local_tasks()
        r = host_exec_cmd(dt, "[v.mgr.host for k, v in get_cluster_info().items()]")
        print(r)
    else:
        print(f"unknown command: {cmd}")


async def sac():
    print(">>> ", end='')
    sys.stdout.flush()
    for line in sys.stdin:
        cmd = line.rstrip()
        await sac_cmd(cmd)
        print(">>> ", end='')
        sys.stdout.flush()


async def entry():
    global default_host
    if default_host.startswith("@"):
        cmd = default_host
        default_host = default_host[1:]
        await sac_cmd(cmd)
    else:
        await sac()


def main():
    asyncio.get_event_loop().run_until_complete(entry())


if __name__ == '__main__':
    main()
