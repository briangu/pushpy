import typing

import tornado.gen
import tornado.httpserver
import tornado.ioloop
import tornado.web
from pysyncobj.batteries import ReplList

from push.mgr.batteries import ReplSyncDict, ReplTimeseries, ReplCodeStore
from push.mgr.code_util import KvStoreLambda, load_src
from push.mgr.push_manager import PushManager
from push.mgr.task import TaskManager


# def create_webserver(base_port, repl_kvstore):
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
    repl_code_store = ReplCodeStore(on_set=None)
    repl_ts = ReplTimeseries(on_append=KvStoreLambda(repl_kvstore, "process_ts_updates"))
    repl_strategies = ReplList()

    tm = TaskManager(repl_kvstore)

    m_globals = dict()
    m_globals['repl_kvstore'] = repl_kvstore
    m_globals['repl_code_store'] = repl_code_store
    m_globals['local_tasks'] = tm
    m_globals['repl_ts'] = repl_ts
    m_globals['repl_strategies'] = repl_strategies
    # m_globals['m_register'] = DoRegister(repl_kvstore)

    return m_globals, make_app(repl_kvstore)


