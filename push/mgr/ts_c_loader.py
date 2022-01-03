import sys

import dill

from push.mgr.push_manager import PushManager

m = PushManager(address=('', int(sys.argv[1])), authkey=b'password')
m.connect()


class Multiplier:
    def apply(self, a, b):
        return a * b


class Adder:
    def apply(self, a, b):
        return a + b


class Interpreter:
    def apply(self, commands):
        from repl_code_store.interpreter import Adder, Multiplier
        print(Adder)
        r = 0
        for i in range(len(commands)):
            cmd = commands[i]
            if cmd == "add":
                r += Adder().apply(commands[i + 1], commands[i + 2])
                i += 2
            elif cmd == "mul":
                r += Multiplier().apply(commands[i + 1], commands[i + 2])
                i += 2
        return r


repl_tasks = m.repl_tasks()
local_tasks = m.local_tasks()

repl_code_store = m.repl_code_store()
repl_code_store.update({
    "interpreter.Interpreter": dill.dumps(Interpreter),
    "interpreter.Adder": dill.dumps(Adder),
    "interpreter.Multiplier": dill.dumps(Multiplier)
}, sync=True)

commands = ['add', 1, 2, 'mul', 3, 4]
r = local_tasks.apply("interpreter.Interpreter", commands)
print(r)
