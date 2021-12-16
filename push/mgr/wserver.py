import multiprocessing

from push.mgr.qm import QueueManager
import asyncio
import tornado.web
import tornado.ioloop
import tornado.httpserver
import tornado.gen

m = QueueManager(address=('', 50000), authkey=b'password')
m.connect()

sync_lock = m.sync_lock()


class MainHandler(tornado.web.RequestHandler):

    @tornado.gen.coroutine
    def get(self):
        self.write('OK')
        self.finish()


class CounterHandler(tornado.web.RequestHandler):

    def initialize(self, sync_lock):
        self.sync_lock = sync_lock

    @tornado.gen.coroutine
    def get(self):
        self.write(str(self.sync_lock.getCounter()))

    # ab -k -c 10 -n 100000 -u foo -T 'application/x-www-form-urlencoded'  http://localhost:11000/counter
    @tornado.gen.coroutine
    def put(self):
        self.write(str(self.sync_lock.incCounter()))

    @tornado.gen.coroutine
    def post(self):
        self.write(str(self.sync_lock.resetCounter()))

class StatusHandler(tornado.web.RequestHandler):
    sync_lock = None

    def initialize(self, sync_lock):
        self.sync_lock = sync_lock

    @tornado.gen.coroutine
    def get(self):
        self.write(str(self.sync_lock.getStatus()))


class ToggleHandler(tornado.web.RequestHandler):
    sync_lock = None

    def initialize(self, sync_lock):
        self.sync_lock = sync_lock

    @tornado.gen.coroutine
    def get(self):
        if self.sync_lock.isOwned("/dog"):
            self.sync_lock.release("/dog")
        else:
            self.sync_lock.tryAcquireLock("/dog")
        self.write("toggled") #str(self.sync_lock.isOwned("/dog")))


def make_app(sync_lock):
    return tornado.web.Application([
        ("/", MainHandler),
        ("/counter", CounterHandler, {'sync_lock': sync_lock}),
        ("/status", StatusHandler, {'sync_lock': sync_lock}),
        ("/toggle", ToggleHandler, {'sync_lock': sync_lock}),
    ])

# app = make_app(sync_lock)
# app.listen(11000)
#

if __name__=='__main__':
    app = make_app(sync_lock)

    server = tornado.httpserver.HTTPServer(app)
    server.bind(11000)

    server.start(int(multiprocessing.cpu_count() * 0.80))

    # tornado.ioloop.IOLoop.current().start()
    loop = asyncio.get_event_loop()
    try:
        loop.run_forever()
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
