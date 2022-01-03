import pickle
import sys
import time
import typing
import uuid
from collections import OrderedDict
from threading import Thread

import dill
import tblib.pickling_support
import gevent
import signal
from geventwebsocket import WebSocketServer, WebSocketApplication, Resource

tblib.pickling_support.install()


class NodeSession:
    def __init__(self):
        self.id = str(uuid.uuid4())
        self.context = {}  # globals().copy()

    def process_message(self, message):
        cmd = chr(message[0]) if type(message) == bytearray else message[0]
        data = message[2:]
        if cmd == '=':
            q = [x.strip() for x in data.split(" ")]
            name = q[0]
            value = " ".join(q[1:])
            # TODO: should this be evaluated in the local or global context?
            v = eval(value, self.context)
            # globals()[name] = v
            self.context[name] = v
            response = None
        elif cmd == 'c':
            response = pickle.dumps(eval(dill.loads(data), self.context))
        elif cmd == 'l':
            response = pickle.dumps(eval(data, self.context))
        else:
            raise RuntimeError(f"unknown command: {cmd}")

        return response

    def close(self):
        pass


# TODO: add ws to sessions so that another session (e.g. REPL can access the ws by session id)
# TODO: add ability for client send session id or receive session id from server -vs making its own up
class SessionManager:
    sessions: typing.Dict[str, NodeSession]

    def __init__(self):
        self.sessions = dict()

    def open(self):
        session = NodeSession()
        self.sessions[session.id] = session
        return session

    def close(self, session_id):
        session = self.sessions.get(session_id)
        if session is not None:
            session.close()
            del self.sessions[session_id]

    def __getitem__(self, session_id):
        return self.sessions.get(session_id)

    def __iter__(self):
        return list(self.sessions.keys()).__iter__()


session_mgr = SessionManager()


# https://stackoverflow.com/questions/49858021/listen-to-multiple-socket-with-websockets-and-asyncio
# https://stackoverflow.com/questions/37946054/modify-python-global-variable-inside-eval
class NodeServerApplication(WebSocketApplication):
    session: typing.Optional[NodeSession]

    def __init__(self, ws):
        super().__init__(ws)
        self.session = None

    def on_open(self):
        self.session = session_mgr.open()
        print(f"open session: {self.session.id} {self.ws}")

    def on_message(self, message, *args, **kwargs):
        if message is None:
            return
        # https://stackoverflow.com/questions/45240549/how-to-serialize-an-exception
        try:
            response = self.session.process_message(message)
            if response is not None:
                self.ws.send(response)
        except Exception as e:
            tblib.pickling_support.install(e)
            res = pickle.dumps(sys.exc_info())
            self.ws.send(res)

    def on_close(self, reason):
        print(f"close session: {self.session.id} reason={reason}")
        session_mgr.close(self.session.id)
        self.session = None
        self.ws.close()
        self.ws = None
        if reason is not None:
            print(reason)


def main(main_control):

    print("main hit")

    gevent.signal_handler(signal.SIGQUIT, gevent.kill)
    gevent.signal_handler(signal.SIGINT, gevent.kill)

    port = 8765  # int(sys.argv[1]) if len(sys.argv) > 1 else 8765

    WebSocketServer(
        ('', port),
        Resource(OrderedDict([('/', NodeServerApplication)]))
    ).serve_forever()


