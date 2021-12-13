from twisted.web import server, resource
from twisted.internet import reactor, endpoints
from autobahn.twisted.websocket import WebSocketClientProtocol, \
    WebSocketClientFactory

import json
import random
import sys

from twisted.python import log
from twisted.internet import reactor


class Counter(resource.Resource):
    isLeaf = True
    numberRequests = 0

    def render_GET(self, request):
        self.numberRequests += 1
        request.setHeader(b"content-type", b"text/plain")
        content = u"I am request #{}\n".format(self.numberRequests)
        return content.encode("ascii")


from twisted.internet.task import deferLater
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from twisted.internet import reactor


ws_client = None
ws_answer = None

class SlowSquareClientProtocol(WebSocketClientProtocol):

    def compute(self):
        x = round(3. * random.random(), 4)
        self.sendMessage(json.dumps(x).encode('utf8'))
        return x

    def onOpen(self):
        x = self.compute()
        print("Request to square {} sent.".format(x))
        global ws_client
        ws_client = self

    def onMessage(self, payload, isBinary):
        if not isBinary:
            res = json.loads(payload.decode('utf8'))
            print("Result received: {}".format(res))
#            self.sendClose()
            global ws_answer
            ws_answer = res

    def onClose(self, wasClean, code, reason):
        if reason:
            print(reason)
        reactor.stop()


class DelayedResource(Resource):
    isLeaf = True

    def _delayedRender(self, request):
        global ws_answer
        request.write(f"<html><body>{ws_answer}</body></html>".encode("ascii"))
        request.finish()

    def render_GET(self, request):
        ws_client.compute()
        d = deferLater(reactor, 5, lambda: request)
        d.addCallback(self._delayedRender)
        return NOT_DONE_YET


endpoints.serverFromString(reactor, "tcp:8080").listen(server.Site(DelayedResource()))

log.startLogging(sys.stdout)

factory = WebSocketClientFactory("ws://127.0.0.1:9000")
factory.protocol = SlowSquareClientProtocol

reactor.connectTCP("127.0.0.1", 9000, factory)

reactor.run()

