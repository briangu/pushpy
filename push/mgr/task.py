import threading
import uuid

from push.mgr.code_util import load_src


class TaskControl:
    running = True


class TaskContext:
    def __init__(self, control, thread):
        self.control = control
        self.thread = thread


class DoTask:
    task_threads = dict()

    def __init__(self, kvstore):
        self.kvstore = kvstore

    def apply(self, src, *args, **kwargs):
        return self.run(task_type="lambda", src=src, *args, **kwargs)

    def run(self, task_type, src, *args, **kwargs):
        src = load_src(self.kvstore, src)
        name = kwargs.get("name")
        name = name or str(uuid.uuid4())
        if name in self.task_threads:
            raise RuntimeError(f"task already running: {name}")
        if task_type == "daemon":
            task_control = TaskControl()
            task_context = TaskContext(task_control,
                                       threading.Thread(target=src, args=(task_control,)))
            task_context.thread.start()
            self.task_threads[name] = task_context
            print(self.task_threads)
        elif task_type == "lambda":
            return src(*args, **kwargs)

    def stop(self, name):
        if name not in self.task_threads:
            return
        self.task_threads[name].control.running = False
        self.task_threads[name].thread.join(timeout=10)
        del self.task_threads[name]
