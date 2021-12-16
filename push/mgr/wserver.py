from push.mgr.qm import QueueManager
import asyncio
import tornado.web

m = QueueManager(address=('', 50000), authkey=b'password')
m.connect()

sync_lock = m.sync_lock()


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world")


class StatusHandler(tornado.web.RequestHandler):
    sync_lock = None

    def initialize(self, sync_lock):
        self.sync_lock = sync_lock

    def get(self):
        self.write(str(self.sync_lock.getStatus()))


class ToggleHandler(tornado.web.RequestHandler):
    sync_lock = None

    def initialize(self, sync_lock):
        self.sync_lock = sync_lock

    def get(self):
        if self.sync_lock.isOwned("/dog"):
            self.sync_lock.release("/dog")
        else:
            self.sync_lock.tryAcquireLock("/dog")
        self.write("toggled") #str(self.sync_lock.isOwned("/dog")))


def make_app(sync_lock):
    return tornado.web.Application([
        (r"/", MainHandler),
        (r"/status", StatusHandler, {'sync_lock': sync_lock}),
        (r"/toggle", ToggleHandler, {'sync_lock': sync_lock}),
    ])

app = make_app(sync_lock)
app.listen(11000)

loop = asyncio.get_event_loop()
try:
    # loop.run_until_complete(main())
    loop.run_forever()
finally:
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()
