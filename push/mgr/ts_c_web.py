import sys

import dill
import tornado.web

from push.mgr.push_manager import PushManager

m = PushManager(address=('', int(sys.argv[1])), authkey=b'password')
m.connect()

repl_code_store = m.repl_code_store()


class HelloHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("hello!")
        self.finish()


class HelloWorldHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("hello, world!")
        self.finish()


repl_code_store.add("/web/", dill.dumps(HelloHandler))
repl_code_store.add("/web/greeting", dill.dumps(HelloWorldHandler))
