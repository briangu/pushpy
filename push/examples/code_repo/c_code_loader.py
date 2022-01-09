import sys

from push.examples.simple_interpreter import Multiplier, Adder, Interpreter
from push.push_manager import PushManager

m = PushManager(address=('', int(sys.argv[1])), authkey=b'password')
m.connect()

local_tasks = m.local_tasks()

repl_code_store = m.repl_code_store()
repl_code_store.update({
    "interpreter.Interpreter": Interpreter,
    "interpreter.math.Adder": Adder,
    "interpreter.math.Multiplier": Multiplier
}, sync=True)

ops = ['add', 'add', 1, 2, 'mul', 3, 4]

# run task via this client
r = local_tasks.apply("interpreter.Interpreter", ops)[0]
print(r)
assert r == 15


class Adder2(Adder):
    def apply(self, a, b):
        print("using adder v2")
        return (a + b) * 2


# TODO: loader needs to trigger on code update and reload
repl_code_store.set("interpreter.math.Adder", Adder2, sync=True)
r = local_tasks.apply("interpreter.Interpreter", ops)[0]
print(r)
assert r == 36
