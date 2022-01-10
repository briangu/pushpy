from push.examples.ex_push_manager import ExamplePushManager
from push.examples.simple_interpreter import Multiplier, Adder, Interpreter

m = ExamplePushManager()
m.connect()

local_tasks = m.local_tasks()

repl_code_store = m.repl_code_store()
repl_code_store.update({
    "interpreter.Interpreter": Interpreter,
    "interpreter.math.Adder": Adder,
    "interpreter.math.Multiplier": Multiplier
}, sync=True)

# run task via this client
commands = ['add', 'add', 1, 2, 'mul', 3, 4]
print(local_tasks.apply("interpreter.Interpreter", commands)[0])
