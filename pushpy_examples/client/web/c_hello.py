import tornado.web

from pushpy_examples.client.ex_push_manager import ExamplePushManager

m = ExamplePushManager()
m.connect()

repl_code_store = m.repl_code_store()


# curl localhost:11000/
class HelloWorldHandler(tornado.web.RequestHandler):
    def get(self):
        from boot_common import repl_code_store
        self.write(f"hello, world!!!! [{repl_code_store.get_head()}]\n")


repl_code_store.set("/web/", HelloWorldHandler, sync=True)
