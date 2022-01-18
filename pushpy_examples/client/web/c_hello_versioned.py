import requests
import tornado.web

from pushpy_examples.client.ex_push_manager import ExamplePushManager

m = ExamplePushManager()
m.connect()

repl_code_store = m.repl_code_store()

web_url = "http://localhost:11000/"


class HelloWorldHandler(tornado.web.RequestHandler):
    def get(self):
        from boot_common import repl_code_store
        self.write(f"hello, world!!!! head=[{repl_code_store.get_head()}]\n")


repl_code_store.set("/web/", HelloWorldHandler, sync=True)
print(requests.get(web_url).text)


class HelloWorldHandler2(tornado.web.RequestHandler):
    def get(self):
        from boot_common import repl_code_store
        self.write(f"Hello, World! (v2) head=[{repl_code_store.get_head()}]\n")

# update and change to v2
repl_code_store.set("/web/", HelloWorldHandler2, sync=True)
print(requests.get(web_url).text)

# revert to the first version of HelloWorldHandler
repl_code_store.set_head(version=repl_code_store.get_head() - 1, sync=True)
print(requests.get(web_url).text)
