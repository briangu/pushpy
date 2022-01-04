import typing
from queue import Queue, Empty

import tornado.gen
import tornado.httpserver
import tornado.ioloop
import tornado.web

from pysyncobj.batteries import ReplList
from tornado.routing import Router

from push.batteries import ReplSyncDict, ReplTimeseries, ReplVersionedDict, ReplTaskManager, CodeStoreLoader
from push.loader import load_src, KvStoreLambda
from push.task_manager import TaskManager


class Handle404(tornado.web.RequestHandler):
    def get(self):
        self.set_status(404)
        self.write('404 Not Found')


# https://stackoverflow.com/questions/47970574/tornado-routing-to-a-base-handler
class MyRouter(Router):
    def __init__(self, store, app, prefix=None):
        self.store = store
        self.app = app
        self.prefix = f"kvstore:{prefix or '/web'}"

    def find_handler(self, request, **kwargs):
        try:
            handler = load_src(self.store, f"{self.prefix}{request.path}") or Handle404
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(e)
            handler = Handle404

        return self.app.get_handler_delegate(request, handler)


def make_app(kvstore):
    return MyRouter(kvstore, tornado.web.Application())


# class DoRegisterCallback:
#     def apply(self, name, src):
#         global onrep
#         src = dill.loads(src)
#         if isinstance(src, type):
#             q = src()
#             onrep.handle_map[name] = q.apply if hasattr(q, 'apply') else q
#         else:
#             onrep.handle_map[name] = src
#
#
# drc = DoRegisterCallback()
# QueueManager.register("do_register_callback", callable=lambda: drc)


# TODO: this should be a replicated command ReplLambda / ReplCommand that runs on all hosts
# class DoRegister:
#     kvstore = None
#
#     def __init__(self, kvstore):
#         self.kvstore = kvstore
#
#     def apply(self, name, src):
#         src = load_src(self.kvstore, src)
#         q = src()
#         PushManager.register(name, callable=lambda l=q: l)


def main() -> (typing.List[object], typing.Dict[str, object]):
    queue = Queue()

    def on_event_daemon(control, handle_map):
        while control.running:
            try:
                s, *a = queue.get(timeout=0.1)
                if s in handle_map:
                    handle_map[s].apply(*a)
            except Empty:
                pass

    repl_code_store = ReplVersionedDict()

    handle_map = {
        'kvstore': KvStoreLambda(repl_code_store, "process_kv_updates"),
        'ts': KvStoreLambda(repl_code_store, "process_ts_updates")
    }

    def on_event_provider(s):
        def on_event(*args):
            queue.put((s, *args))
        return on_event

    repl_kvstore = ReplSyncDict(on_set=on_event_provider('kvstore'))
    repl_ts = ReplTimeseries(on_append=on_event_provider('ts'))
    repl_strategies = ReplList()

    tm = TaskManager(repl_code_store)
    tm.start_daemon(on_event_daemon, handle_map)

    repl_task_manager = ReplTaskManager(repl_kvstore, tm)

    m_globals = dict()
    m_globals['repl_kvstore'] = repl_kvstore
    m_globals['repl_code_store'] = repl_code_store
    m_globals['repl_tasks'] = repl_task_manager
    m_globals['local_tasks'] = tm
    m_globals['repl_ts'] = repl_ts
    m_globals['repl_strategies'] = repl_strategies
    # m_globals['m_register'] = DoRegister(repl_kvstore)

    CodeStoreLoader.install_importer({'repl_code_store': repl_code_store})

    return m_globals, make_app(repl_code_store)
