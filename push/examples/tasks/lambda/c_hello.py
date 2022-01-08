import sys

from push.push_manager import PushManager

m = PushManager(address=('', int(sys.argv[1])), authkey=b'password')
m.connect()

# print on connected 'local' host
dt = m.local_tasks()
dt.apply(lambda: print("hello from local!"))

# print on all hosts (host_id is in scope when running on the remote host)
dt = m.repl_tasks()
dt.apply(lambda: print(f"hello from {host_id}!"))

