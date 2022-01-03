import typing

import tornado.gen
import tornado.httpserver
import tornado.ioloop
import tornado.web

from pysyncobj.batteries import ReplList
from tornado.routing import Router

from push.mgr.batteries import ReplSyncDict, ReplTimeseries, ReplVersionedDict, ReplTaskManager, CodeStoreLoader
from push.mgr.code_util import KvStoreLambda, load_src
from push.mgr.task_manager import TaskManager


class Handle404(tornado.web.RequestHandler):
    def get(self):
        self.set_status(404)
        self.write('404 Not Found')


# https://stackoverflow.com/questions/47970574/tornado-routing-to-a-base-handler
class MyRouter(Router):
    def __init__(self, kvstore, app, prefix=None):
        self.kvstore = kvstore
        self.app = app
        self.prefix = f"kvstore:{prefix or '/web'}"

    def find_handler(self, request, **kwargs):
        try:
            handler = load_src(self.kvstore, f"{self.prefix}{request.path}") or Handle404
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
    repl_kvstore = ReplSyncDict(on_set=None)
    repl_code_store = ReplVersionedDict()
    repl_ts = ReplTimeseries(on_append=KvStoreLambda(repl_code_store, "process_ts_updates"))
    repl_strategies = ReplList()

    tm = TaskManager(repl_code_store)
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
