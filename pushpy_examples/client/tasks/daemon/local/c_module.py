import time

from pushpy_examples.client.ex_push_manager import ExamplePushManager
from client.simple_interpreter import Multiplier, Adder, Interpreter

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
            ops = ['add', 'add', random.randint(0, 10), random.randint(0, 10), 'mul', random.randint(0, 10), random.randint(0, 10)]
            print("task: ", local_tasks.apply("interpreter.Interpreter", ops)[0])

            from repl_code_store.interpreter import Interpreter
            print("import: ", Interpreter().apply(ops=ops)[0])

            time.sleep(1)


repl_code_store.set("my_math_daemon_task", RandomMathDaemonTask, sync=True)

dt = m.local_tasks()
dt.stop("mmdt")
dt.run("daemon", src="my_math_daemon_task", name="mmdt")

time.sleep(30)
dt.stop("mmdt")

