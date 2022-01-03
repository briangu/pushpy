from twisted.web import server, resource
from twisted.internet import reactor, endpoints
from autobahn.twisted.websocket import WebSocketClientProtocol, WebSocketClientFactory

from twisted.internet.protocol import ReconnectingClientFactory

import json
import random
import sys

from twisted.python import log
from twisted.internet import reactor
from twisted.internet.defer import DeferredQueue
from twisted.internet.task import deferLater
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from twisted.internet import reactor


dqreq = DeferredQueue()
dqres = DeferredQueue()

def compute():
    return round(3. * random.random(), 4)

# https://stackoverflow.com/questions/35357354/how-to-use-kafka-on-tornado
# https://medium.com/@benjaminmbrown/real-time-data-visualization-with-d3-crossfilter-and-websockets-in-python-tutorial-dba5255e7f0e
# https://travishorn.com/d3-line-chart-with-forecast-90507cb27ef2

class SlowSquareClientProtocol(WebSocketClientProtocol):

    d = None

    def subscribe(self):
        global dqreq
        self.d = dqreq.get()
        self.d.addCallback(lambda x: self.sendMessage(json.dumps(x).encode('utf8')))

    def onOpen(self):
        self.subscribe()

    def onMessage(self, payload, isBinary):
        if not isBinary:
            res = json.loads(payload.decode('utf8'))
            global dqres
            dqres.put(res) 
            self.subscribe()

    def onClose(self, wasClean, code, reason):
        if reason:
            print(reason)
        if self.d is not None:
            self.d.cancel()
            self.d = None
#        reactor.stop()


class DelayedResource(Resource):
    isLeaf = True

    def _delayedRender(self, request, x):
        request.write(f"<html><body>{x}</body></html>".encode("utf-8"))
        request.finish()

    def render_GET(self, request):
        dqreq.put(compute())
        d = dqres.get()
        d.addCallback(lambda x: self._delayedRender(request, x))
        return NOT_DONE_YET


class MyClientFactory(WebSocketClientFactory, ReconnectingClientFactory):

    protocol = SlowSquareClientProtocol 

    def clientConnectionFailed(self, connector, reason):
        print("Client connection failed .. retrying ..")
        self.retry(connector)

    def clientConnectionLost(self, connector, reason):
        print("Client connection lost .. retrying ..")
        self.retry(connector)


endpoints.serverFromString(reactor, "tcp:8080").listen(server.Site(DelayedResource()))

#log.startLogging(sys.stdout)

factory = MyClientFactory("ws://127.0.0.1:9000")

reactor.connectTCP("127.0.0.1", 9000, factory)

reactor.run()

