import sys

import tornado.web

from push.push_manager import PushManager

m = PushManager(address=('', int(sys.argv[1])), authkey=b'password')
m.connect()

repl_code_store = m.repl_code_store()

# revert to the first version of HelloWorldHandler
repl_code_store.set_head(version=repl_code_store.get_head()-1, sync=True)
