import sys

from pushpy.push_manager import PushManager
from pushpy.push_server_utils import host_to_address


class ExamplePushManager(PushManager):

    def __init__(self, *args, **kwargs):
        host = sys.argv[1] if len(sys.argv) > 1 else "localhost:50000"
        auth_key = bytes(sys.argv[2]) if len(sys.argv) > 2 else b'password'
        super().__init__(*args, address=host_to_address(host), authkey=auth_key, **kwargs)

