import typing

import tornado.gen
import tornado.httpserver
import tornado.ioloop
import tornado.web
from pysyncobj.batteries import ReplList

from push.mgr.batteries import ReplSyncDict, ReplTimeseries
from push.mgr.code_util import KvStoreLambda, load_src
# from push.mgr.qm import QueueManager
from push.mgr.task import TaskManager


def create_webserver(base_port, repl_kvstore):
    class MainHandler(tornado.web.RequestHandler):

        kvstore = None

        def initialize(self, kvstore):
            self.kvstore = kvstore

        @tornado.gen.coroutine
        def get(self):
            on_get_v = self.kvstore.get("on_get_v")
            if on_get_v is not None:
                kv_on_get = self.kvstore.get(f"on_get_v{on_get_v}")
                if kv_on_get is not None:
                    kv_on_get = load_src(self.kvstore, kv_on_get)
                    kv_on_get(self)

    def make_app(kvstore):
        return tornado.web.Application([
            ("/", MainHandler, {'kvstore': kvstore})
        ])

    webserver = tornado.httpserver.HTTPServer(make_app(repl_kvstore))
    web_port = (base_port % 1000) + 11000
    print(f"my web port: {web_port}")
    webserver.listen(web_port)


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


# class DoRegister:
#     def apply(self, name, src):
#         src = load_src(repl_kvstore, src)
#         q = src()
#         QueueManager.register(name, callable=lambda: q)
#
#
# dr = DoRegister()


def create_subconsumers(base_port) -> (typing.List[object], typing.Dict[str, object]):
    repl_kvstore = ReplSyncDict(on_set=None)
    repl_ts = ReplTimeseries(on_append=KvStoreLambda(repl_kvstore, "process_ts_updates").apply)
    repl_strategies = ReplList()

    tm = TaskManager(repl_kvstore)

    qm_globals = dict()
    qm_globals['repl_kvstore'] = repl_kvstore
    qm_globals['repl_tasks'] = tm
    qm_globals['repl_ts'] = repl_ts
    qm_globals['repl_strategies'] = repl_strategies

    qm_methods = dict()
    qm_methods['kvstore'] = lambda: repl_kvstore
    qm_methods['tasks'] = lambda: tm
    qm_methods['ts'] = lambda: repl_ts
    qm_methods['strategies'] = lambda: repl_strategies

    # QueueManager.register('kvstore', callable=lambda: repl_kvstore)
    # QueueManager.register('tasks', callable=lambda: tm)
    # QueueManager.register('ts', callable=lambda: repl_ts)
    # QueueManager.register('strategies', callable=lambda: repl_strategies)

    create_webserver(base_port, repl_kvstore)

    return [repl_ts, repl_kvstore, repl_strategies], qm_globals, qm_methods

