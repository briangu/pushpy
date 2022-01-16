class Multiplier:
    def apply(self, a, b):
        return a * b


class Adder:
    def apply(self, a, b):
        return a + b


class Interpreter:
    def apply(self, ops, i=None):
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
