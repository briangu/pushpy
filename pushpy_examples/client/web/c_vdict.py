import tornado.web

from pushpy_examples.client.ex_push_manager import ExamplePushManager

m = ExamplePushManager()
m.connect()

repl_code_store = m.repl_code_store()


# curl -X PUT -d'{"greeting":"Hello!"}' -H 'Content-Type: application/json' localhost:11000/greeting
# curl -X PUT -d'{"greeting":"Hello, World!"}' -H 'Content-Type: application/json' localhost:11000/greeting
# curl localhost:11000/greeting
# curl localhost:11000/greeting?v=1
class VersionedStoreHandler(tornado.web.RequestHandler):
    def get(self):
        from boot_common import repl_ver_store
        if 'v' in self.request.arguments:
            v = self.request.arguments.get('v')[0].decode('utf-8')
            repl_ver_store.set_head(version=int(v), sync=True)
        self.write(str(repl_ver_store.get("greeting"))+"\n")
        self.finish()

    def put(self):
        import json
        from boot_common import repl_ver_store
        body = json.loads(self.request.body)
        greeting = body.get('greeting')
        if greeting is not None:
            repl_ver_store.set("greeting", greeting, sync=True)
        self.write(f"max_version={repl_ver_store.get_max_version()}\n")
        self.finish()

    def delete(self):
        from boot_common import repl_ver_store
        repl_ver_store.clear()
        self.finish()


repl_code_store.set("/web/greeting", VersionedStoreHandler)
