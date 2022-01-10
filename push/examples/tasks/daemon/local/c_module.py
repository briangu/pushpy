import time

from push.examples.ex_push_manager import ExamplePushManager
from push.examples.simple_interpreter import Multiplier, Adder, Interpreter

m = ExamplePushManager()
m.connect()

# load a set of classes into the code store to be resolved by the module loader
repl_code_store = m.repl_code_store()
repl_code_store.update({
    "interpreter.Interpreter": Interpreter,
    "interpreter.math.Adder": Adder,
    "interpreter.math.Multiplier": Multiplier
}, sync=True)


class RandomMathDaemonTask:
    def apply(self, control):
        import random
        import time

        while control.running:
            commands = ['add', 'add', random.randint(0, 10), random.randint(0, 10), 'mul', random.randint(0, 10), random.randint(0, 10)]
            print(local_tasks.apply("interpreter.Interpreter", commands)[0])
            time.sleep(1)


repl_code_store.set("my_adder_task", RandomMathDaemonTask, sync=True)

dt = m.local_tasks()
dt.stop("mmdt")
dt.run("daemon", src="my_math_daemon_task", name="mmdt")

time.sleep(30)
dt.stop("mmdt")

