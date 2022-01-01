import sys

import dill
import tornado.web
import json

from push.mgr.push_manager import PushManager

m = PushManager(address=('', int(sys.argv[1])), authkey=b'password')
m.connect()

repl_code_store = m.repl_code_store()


class HelloHandler(tornado.web.RequestHandler):
    def get(self):
        print(self.request.arguments)
        if 'k' in self.request.arguments:
            k = self.request.arguments['k'][0].decode('utf-8')
            self.write(str(repl_kvstore.get(k)))
        else:
            self.write(str(list(repl_kvstore.keys())))
        self.finish()

    def put(self):
        import json
        print(self.request.body)
        body = json.loads(self.request.body)
        k = body.get('k')
        v = body.get('v')
        print(k, v)
        if k is not None and v is not None:
            repl_kvstore.set(k, v)


class HelloWorldHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("hello, world!")
        self.finish()


repl_code_store.commit("/web/kv", dill.dumps(HelloHandler))
repl_code_store.commit("/web/", dill.dumps(HelloWorldHandler))
