import tornado.web

from pushpy_examples.client.ex_push_manager import ExamplePushManager

m = ExamplePushManager()
m.connect()

repl_code_store = m.repl_code_store()


# curl -X PUT -d'{"k":"my_key", "v":"my_value"}' -H 'Content-Type: application/json' localhost:11000/kv
# curl localhost:11000/kv?k=my_key
class StoreHandler(tornado.web.RequestHandler):
    def get(self):
        from boot_common import repl_kvstore
        if 'k' in self.request.arguments:
            k = self.request.arguments['k'][0].decode('utf-8')
            self.write(str(repl_kvstore.get(k)))
        else:
            self.write(str(list(repl_kvstore.keys())))
        self.write("\n")
        self.finish()

    def put(self):
        import json
        from boot_common import repl_kvstore
        body = json.loads(self.request.body)
        k = body.get('k')
        v = body.get('v')
        if k is not None and v is not None:
            repl_kvstore.set(k, v)


repl_code_store.set("/web/kv", StoreHandler, sync=True)
