import sys

import tornado.web

from push.push_manager import PushManager
from push.examples.simple_interpreter import Interpreter, Adder, Multiplier

m = PushManager(address=('', int(sys.argv[1])), authkey=b'password')
m.connect()

repl_code_store = m.repl_code_store()

repl_tasks = m.repl_tasks()
local_tasks = m.local_tasks()

repl_code_store.update({
    "interpreter.Interpreter": Interpreter,
    "interpreter.math.Adder": Adder,
    "interpreter.math.Multiplier": Multiplier
}, sync=True)


# curl -X PUT -d'{"k":"my_key", "v":"my_value"}' -H 'Content-Type: application/json' localhost:11000/kv
# curl localhost:11000/kv?k=my_key
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


# curl localhost:11000/
class HelloWorldHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("hello, world!")
        self.finish()


# curl -X POST -d'["add", "add", 1, 2, "mul", 3, 4]' -H 'Content-Type: application/json' localhost:11000/math
class MathHandler(tornado.web.RequestHandler):
    def post(self):
        import json
        ops = json.loads(self.request.body.decode("utf-8"))
        r = local_tasks.apply("interpreter.Interpreter", ops)[0]
        self.write(str(r))
        self.finish()


repl_code_store.set("/web/math", MathHandler)
repl_code_store.set("/web/kv", HelloHandler)
repl_code_store.set("/web/", HelloWorldHandler)
