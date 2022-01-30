from pushpy.code_store import packages_to_dict, show_dict
from pushpy_examples.client.ex_push_manager import ExamplePushManager
from pushpy_examples.client.simple_interpreter import Multiplier, Adder, Interpreter

m = ExamplePushManager()
m.connect()

local_tasks = m.local_tasks()

repl_code_store = m.repl_code_store()
code = packages_to_dict({
    "interpreter.Interpreter": Interpreter,
    "interpreter.math.Adder": Adder,
    "interpreter.math.Multiplier": Multiplier
})
show_dict(code)
repl_code_store.update(code, sync=True)

ops = ['add', 'add', 1, 2, 'mul', 3, 4]

def run_interp(x):
#    from interpreter import Interpreter
    import interpreter
    return interpreter.Interpreter().apply(x)

# run task via this client
r = local_tasks.apply(run_interp, ops)[0]
print(r)
assert r == 15


class Adder2(Adder):
    def apply(self, a, b):
        print("using adder v2")
        return (a + b) * 2


code2 = packages_to_dict({
    "interpreter.Interpreter": Interpreter,
    "interpreter.math.Adder": Adder2,
    "interpreter.math.Multiplier": Multiplier
})
repl_code_store.update(code2, sync=True)
r = local_tasks.apply(run_interp, ops)[0]
print(r)
assert r == 36


class InterpreterWrapper:
    def apply(self, ops):
        print("InterpreterWrapper")
        from interpreter import Interpreter
        return Interpreter().apply(ops)


r = local_tasks.apply(InterpreterWrapper, ops)[0]
print(r)
assert r == 36
