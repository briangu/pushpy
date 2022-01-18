from pushpy_examples.client.ex_push_manager import ExamplePushManager

m = ExamplePushManager()
m.connect()


class MathProcess:
    def apply(self, k):
        import math
        return math.pi * k


class MathProcess2:
    def apply(self, k):
        import math
        return math.pi + k


repl_tasks = m.repl_tasks()
local_tasks = m.local_tasks()


def test_code(expected, key, *args, **kwargs):
    assert expected == repl_tasks.apply(key, *args, **kwargs, sync=True)
    assert expected == local_tasks.apply(key, *args, **kwargs)


repl_code_store = m.repl_code_store()
repl_code_store.set("math_process", MathProcess, sync=True)
[test_code(MathProcess().apply(i), "math_process", i) for i in range(2)]
repl_code_store.set("math_process", MathProcess2, sync=True)
[test_code(MathProcess2().apply(i), "math_process", i) for i in range(2)]
v = repl_code_store.get_head()
repl_code_store.set_head(v - 1, sync=True)
[test_code(MathProcess().apply(i), "math_process", i) for i in range(2)]
repl_code_store.set_head(v, sync=True)


def do_pi(k):
    import math
    return math.pi * k


v = repl_code_store.get_head()
repl_code_store.set("my_lambda", do_pi, sync=True)
assert repl_code_store.get_head() == v + 1
[test_code(do_pi(i), "my_lambda", i) for i in range(2)]

my_lambda = lambda x: x * 2

v = repl_code_store.get_head()
repl_code_store.set("my_lambda", my_lambda, sync=True)
assert repl_code_store.get_head() == v + 1

[test_code(my_lambda(i), "my_lambda", i) for i in range(2)]

v = repl_code_store.get_head()
repl_code_store.set_head(v - 1, sync=True)
assert repl_code_store.get_head() == v - 1
v = repl_code_store.get_head()
[test_code(do_pi(i), "my_lambda", i) for i in range(2)]

# expect None response
assert repl_tasks.apply("my_lambda", 3) is None
assert repl_tasks.apply("my_lambda", 4) is None

# TODO: can create tool that loads/saves directory path into store
for k in repl_code_store.keys():
    print(k)
