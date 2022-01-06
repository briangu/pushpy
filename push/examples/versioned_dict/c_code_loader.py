import sys
import time

from push.push_manager import PushManager
from simple_interpreter import Multiplier, Adder, Interpreter

m = PushManager(address=('', int(sys.argv[1])), authkey=b'password')
m.connect()

repl_tasks = m.repl_tasks()
local_tasks = m.local_tasks()

repl_code_store = m.repl_code_store()
repl_code_store.update({
    "interpreter.Interpreter": Interpreter,
    "interpreter.math.Adder": Adder,
    "interpreter.math.Multiplier": Multiplier
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


repl_code_store.set("my_adder_task", AdderTask, sync=True)

dt = m.local_tasks()
dt.stop("mat")
dt.run("daemon", src="kvstore:my_adder_task", name="mat")

time.sleep(30)
dt.stop("mat")
