import sys

import tornado.web

from push.push_manager import PushManager

m = PushManager(address=('', int(sys.argv[1])), authkey=b'password')
m.connect()

repl_code_store = m.repl_code_store()

# curl -X PUT -d'{"k":"my_key", "v":"my_value"}' -H 'Content-Type: application/json' localhost:11000/kv
# curl localhost:11000/kv?k=my_key
class StoreHandler(tornado.web.RequestHandler):
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


repl_code_store.set("/web/kv", StoreHandler)
