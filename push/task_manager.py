import threading
import uuid

from push.code_util import load_lambda


class TaskControl:
    running = True


class TaskContext:
    def __init__(self, control, thread):
        self.control = control
        self.thread = thread


class TaskManager:
    task_threads = dict()

    # TODO: add ability to store result of code store lambda in kvstore
    def __init__(self, code_store):
        self.code_store = code_store

    def apply(self, src, *args, ctx=None, **kwargs):
        src = (src if src.startswith("kvstore:") else f"kvstore:{src}") if isinstance(src, str) else src
        src = load_lambda(self.code_store, src)
        if src is not None:
            ctx = ctx.copy() if ctx is not None else {}
            ctx.update({'src': src, 'args': args, 'kwargs': kwargs})
            # TOOD: support lambda requirements
            # exec('import math', ctx)
            try:
                exec(f"__r = src(*args, **kwargs)", ctx)
                return ctx['__r']
            except Exception as e:
                print(e)
                return e
        return None

    def start_daemon(self, src, *args, ctx=None, **kwargs):
        name = kwargs.get("name")
        name = name or str(uuid.uuid4())
        if name in self.task_threads:
            raise RuntimeError(f"task already running: {name}")
        src = load_lambda(self.code_store, src)
        task_control = TaskControl()
        task_context = TaskContext(task_control,
                                   threading.Thread(target=src, args=(task_control,)))
        task_context.thread.start()
        self.task_threads[name] = task_context

    def stop(self, name):
        if name not in self.task_threads:
            return
        self.task_threads[name].control.running = False
        self.task_threads[name].thread.join(timeout=10)
        del self.task_threads[name]

    def run(self, task_type, src, *args, ctx=None, **kwargs):
        if task_type == "daemon":
            self.start_daemon(src, *args, ctx=ctx, **kwargs)
        elif task_type == "lambda":
            return self.apply(src, *args, ctx=ctx, **kwargs)

