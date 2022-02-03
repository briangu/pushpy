import time

from pushpy_examples.client.ex_push_manager import ExamplePushManager

m = ExamplePushManager()
m.connect()


class ScheduleTask:
    def apply(self, control):
        import schedule
        import time

        def job():
            print(f"I'm working...{time.time()}")

        schedule.clear()
        schedule.every(5).seconds.do(job)
        # schedule.every(10).minutes.do(job)
        # schedule.every().hour.do(job)
        # schedule.every().day.at("10:30").do(job)
        # schedule.every(5).to(10).minutes.do(job)
        # schedule.every().monday.do(job)
        # schedule.every().wednesday.at("13:15").do(job)
        # schedule.every().minute.at(":17").do(job)

        while control.running:
            schedule.run_pending()
            time.sleep(1)


repl_code_store = m.repl_code_store()
repl_code_store.set("schedule_task", ScheduleTask, sync=True)

dt = m.local_tasks()
dt.stop("schedule_task")
dt.run("daemon", src="schedule_task", name="schedule_task")

time.sleep(30)

dt.stop("schedule_task")
