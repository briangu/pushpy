import time

from pushpy_examples.client.ex_push_manager import ExamplePushManager

m = ExamplePushManager()
m.connect()


class DaemonTask:

    def apply(self, control):
        import time
        while control.running:
            print(time.time())
            time.sleep(1)


repl_code_store = m.repl_code_store()
repl_code_store.set("my_daemon_task", DaemonTask, sync=True)

dt = m.local_tasks()
dt.stop("mdt")
dt.run("daemon", src="my_daemon_task", name="mdt")

time.sleep(30)

dt.stop("mdt")
