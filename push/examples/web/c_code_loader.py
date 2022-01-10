import tornado.web

from push.examples.ex_push_manager import ExamplePushManager
from push.examples.simple_interpreter import Interpreter, Adder, Multiplier

m = ExamplePushManager()
m.connect()

repl_code_store = m.repl_code_store()

repl_code_store.update({
    "interpreter.Interpreter": Interpreter,
    "interpreter.math.Adder": Adder,
    "interpreter.math.Multiplier": Multiplier
}, sync=True)


# curl -X POST -d'["add", "add", 1, 2, "mul", 3, 4]' -H 'Content-Type: application/json' localhost:11000/math
class MathHandler(tornado.web.RequestHandler):
    def post(self):
        import json
        ops = json.loads(self.request.body.decode("utf-8"))
        r = local_tasks.apply("interpreter.Interpreter", ops)[0]
        self.write(str(r))
        self.write("\n")
        self.finish()


repl_code_store.set("/web/math", MathHandler, sync=True)
