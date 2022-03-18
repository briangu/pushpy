import os
import re
import threading
from multiprocessing import process


def host_to_address(host):
    p = host.split(":")
    return (p[0], int(p[1])) if len(p) == 2 else ('', int(p[-1]))


# Reimplementation of the BaseManager serve_forever that returns
def serve_forever(mgmt_server):
    mgmt_server.stop_event = threading.Event()
    process.current_process()._manager_server = mgmt_server
    try:
        accepter = threading.Thread(target=mgmt_server.accepter, daemon=True)
        accepter.start()
        return accepter
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(e)


def load_config(config_fname):
    import yaml

    with open(config_fname, "r") as stream:
        try:
            path_matcher = re.compile(r'.*\$\{([^}^{]+)\}.*')

            def path_constructor(loader, node):
                return os.path.expandvars(node.value)

            class EnvVarLoader(yaml.FullLoader):
                pass

            EnvVarLoader.add_implicit_resolver('!env', path_matcher, None)
            EnvVarLoader.add_constructor('!env', path_constructor)

            d = stream.read()
            print(type(d))
            print(d)

            return yaml.load(d, Loader=EnvVarLoader)
        except yaml.YAMLError as exc:
            print(exc)

    return None
