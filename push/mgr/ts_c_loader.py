import sys
import time

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
    def apply(self, ops, i = None):
        from repl_code_store.interpreter.math import Adder, Multiplier
        i = 0 if i is None else i
        while i < len(ops):
            op = ops[i]
            i += 1
            if op == "add":
                a, i = self.apply(ops, i)
                b, i = self.apply(ops, i)
                return Adder().apply(a, b), i
            elif op == "mul":
                a, i = self.apply(ops, i)
                b, i = self.apply(ops, i)
                return Multiplier().apply(a, b), i
            else:
                return op, i


repl_tasks = m.repl_tasks()
local_tasks = m.local_tasks()

repl_code_store = m.repl_code_store()
repl_code_store.update({
    "interpreter.Interpreter": dill.dumps(Interpreter),
    "interpreter.math.Adder": dill.dumps(Adder),
    "interpreter.math.Multiplier": dill.dumps(Multiplier)
}, sync=True)

commands = ['add', 'add', 1, 2, 'mul', 3, 4]
print(local_tasks.apply("interpreter.Interpreter", commands)[0])


class AdderTask:
    def apply(self, control):
        print("my_adder_task daemon here! 1")

        import random
        import time

        while control.running:
            commands = ['add', 'add', random.randint(0, 10), random.randint(0, 10), 'mul', random.randint(0, 10), random.randint(0, 10)]
            print(local_tasks.apply("interpreter.Interpreter", commands)[0])
            time.sleep(1)

repl_code_store.set("my_adder_task", dill.dumps(AdderTask), sync=True)

dt = m.local_tasks()
dt.stop("mat")
dt.run("daemon", src="kvstore:my_adder_task", name="mat")

time.sleep(30)
dt.stop("mat")
