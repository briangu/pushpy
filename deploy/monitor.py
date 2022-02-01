#!/usr/bin/env python
from __future__ import print_function

import argparse
import logging
import os
import signal
import sys
import threading
import time

import daemon
import daemon.pidfile

# adapted from https://raw.githubusercontent.com/ggmartins/dataengbb/master/python/daemon/daemon1
from pushpy.code_store import ensure_path, load_module

PATHCTRL = '/tmp/pushpy'  # path to control files pid and lock
parser = argparse.ArgumentParser(prog="monitor")

sp = parser.add_subparsers()
sp_start = sp.add_parser('start', help='Starts %(prog)s daemon')
sp_start.add_argument('name', type=str, help='name of daemon')
sp_start.add_argument('path', type=str, help='path to daemon main file')
sp_start.add_argument('-v', '--verbose', action='store_true', help='log extra information')

sp_stop = sp.add_parser('stop', help='Stops %(prog)s daemon')
sp_stop.add_argument('name', type=str, help='name of daemon')

sp_status = sp.add_parser('status', help='Show the status of %(prog)s daemon')
sp_status.add_argument('name', type=str, help='name of daemon')

sp_restart = sp.add_parser('restart', help='Restarts %(prog)s daemon')
sp_restart.add_argument('name', type=str, help='name of daemon')

sp_debug = sp.add_parser('debug', help='Starts %(prog)s daemon in debug mode')
sp_debug.add_argument('-v', '--verbose', action='store_true', help='log extra information')
sp_debug.add_argument('name', type=str, help='name of daemon')
sp_debug.add_argument('path', type=str, help='path to daemon main file')


class MainCtrl:
    thread_continue = True
    # thread_token = "token"


def main_thread(args, mainctrl, log):
    verbose = False

    if hasattr(args, 'verbose'):
        verbose = args.verbose

    if verbose:
        log.info("ARGS:{0}".format(args))

    try:
        with open(main_path, "r") as f:
            module_name = f.read()
        load_module_and_run(module_name, log, mainctrl)
    except KeyboardInterrupt as ke:
        if verbose:
            log.warning("Interrupting...")
    except Exception as e:
        if verbose:
            import traceback
            traceback.print_exc()
            log.error("Exception:{0}".format(str(e)))
    log.info("Exiting...")


def daemon_start(args):
    mainctrl = MainCtrl()

    def main_thread_stop(signum=None, frame=None):
        mainctrl.thread_continue = False
        threading.main_thread().is_alive()
        # mainctrl.thread_token = "test"
        # print("TOKEN:{0}".format(mainctrl.thread_token))

    if not os.path.exists(main_path):
        with open(main_path, "w") as f:
            f.write(args.path)

    print(f"INFO: {args.name} Starting ... {tmp_path}")
    if os.path.exists(pidpath):
        print("INFO: {0} already running (according to {1}).".format(args.name, pidpath))
        sys.exit(1)

    with open(log_stdout, 'w') as f_stdout:
        with open(log_stderr, 'w') as f_stderr:
            with daemon.DaemonContext(
                    stdout=f_stdout,
                    stderr=f_stderr,
                    signal_map={
                        signal.SIGTERM: main_thread_stop,
                        signal.SIGTSTP: main_thread_stop,
                        signal.SIGINT: main_thread_stop,
                        # signal.SIGKILL: daemon_stop, #SIGKILL is an Invalid argument
                        signal.SIGUSR1: daemon_status,
                        signal.SIGUSR2: daemon_status,
                    },
                    pidfile=daemon.pidfile.PIDLockFile(pidpath)
            ):
                logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                                    datefmt='%Y-%m-%dT%H:%M:%S',
                                    filename=logpath,
                                    # filemode='w',
                                    level=logging.INFO)

                log = logging.getLogger(__name__)
                main_thread(args, mainctrl, log)


def daemon_restart(args):
    print("INFO: {0} Restarting...".format(args.name))
    daemon_stop(args)

    if not os.path.exists(main_path):
        raise RuntimeError(f"missing main module path: {main_path}")

    while os.path.exists(pidpath):
        time.sleep(1)

    daemon_start(args)


def daemon_stop(args):
    print("INFO: {0} Stopping with args {1}".format(args.name, args))
    if os.path.exists(pidpath):
        with open(pidpath) as pid:
            try:
                os.kill(int(pid.readline()), signal.SIGINT)
            except ProcessLookupError as ple:
                os.remove(pidpath)
                print("ERROR ProcessLookupError: {0}".format(ple))
    else:
        print("ERROR: process isn't running (according to the absence of {0}).".format(pidpath))


def daemon_debug(args):
    print("INFO: running in debug mode.")
    if not os.path.exists(main_path):
        with open(main_path, "w") as f:
            f.write(args.path)
    log = logging.getLogger(__name__)
    mainctrl = MainCtrl()
    main_thread(args, mainctrl, log)


def daemon_status(args):
    print("INFO: {0} Status {1}".format(args.name, args))
    if os.path.exists(pidpath):
        print("INFO: {0} is running".format(args.name))
    else:
        print("INFO: {0} is NOT running.".format(args.name))


sp_stop.set_defaults(callback=daemon_stop)
sp_status.set_defaults(callback=daemon_status)
sp_start.set_defaults(callback=daemon_start)
sp_restart.set_defaults(callback=daemon_restart)
sp_debug.set_defaults(callback=daemon_debug)

args = parser.parse_args()

tmp_path = os.path.join(PATHCTRL, args.name)
ensure_path(tmp_path)

logpath = os.path.join(tmp_path, args.name + ".log")
log_stdout = os.path.join(tmp_path, args.name + ".out")
log_stderr = os.path.join(tmp_path, args.name + ".err")
pidpath = os.path.join(tmp_path, args.name + ".pid")
main_path = os.path.join(tmp_path, args.name + ".main.path")

if hasattr(args, 'callback'):
    args.callback(args)
else:
    parser.print_help()


def load_module_and_run(m, log, *args, **kwargs):
    if not os.path.exists(m):
        orig_m = m
        m = os.path.join(os.path.dirname(__file__), m)
        log.warn(f"{orig_m} not found, using current dir for {m}")
        if not os.path.exists(m):
            log.error(f"module not found: {m}")
            raise RuntimeError(f"module not found: {m}")
    log.info(f"loading and running: {m}")
    module = load_module(m)
    if 'main' not in module.__dict__:
        log.error(f"missing main function in module: {m}")
        raise RuntimeError(f"missing main function in module: {m}")
    module.__dict__['main'](*args, **kwargs)