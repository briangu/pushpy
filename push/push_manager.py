from multiprocessing.managers import BaseManager, listener_client

import dill


# def create_client_override():
#     m = listener_client['pickle'][1]
#
#     def __convert(a):
#         print(type(a), a)
#         if isinstance(a, list):
#             return [__convert(x) for x in a]
#         elif isinstance(a, tuple):
#             return tuple(__convert(list(a)))
#         elif isinstance(a, dict):
#             return {k: __convert(v) for k, v in a}
#         return dill.dumps(a) if isinstance(a, type) or callable(a) else a
#
#     def override_client(*args, **kwargs):
#         x = m(*args, **kwargs)
#         x.__send = x.send
#
#         def _send(*_args, **_kwargs):
#             _args = __convert(_args)
#             _kwargs = __convert(_kwargs)
#             x.__send(*_args, **_kwargs)
#
#         x.send = _send
#         return x
#
#     return override_client

def create_client_override():
    m = listener_client['pickle'][1]

    def __convert(a):
        if isinstance(a, list):
            return [__convert(x) for x in a]
        elif isinstance(a, tuple):
            return tuple(__convert(list(a)))
        elif isinstance(a, dict):
            return {k: __convert(v) for k, v in a.items()}
        return dill.dumps(a) if isinstance(a, type) or callable(a) else a

    def override_client(*args, **kwargs):
        x = m(*args, **kwargs)
        x.__send = x.send

        def _send(obj):
            # print(f"sending: ", obj)
            # o = dill.dumps(__convert(obj))
            # print(o)
            x.__send(__convert(obj))
            # print(f"sent: {o}")

        x.send = _send
        return x

    return override_client


def create_listener_override():
    m = listener_client['pickle'][0]

    def override_listener(*args, **kwargs):
        x = m(*args, **kwargs)
        x.__accept = x.accept

        def _accept(*args, **kwargs):
            c = x.__accept(*args, **kwargs)
            # print(c)
            c.__recv = c.recv

            def _recv(*_args, **_kwargs):
                r = c.__recv(*_args, **_kwargs)
                obj = dill.loads(r)
                # print(f"r: {r} {obj}")
                return obj

            c.recv = _recv
            return c

        x.accept = _accept

        return x

    return override_listener


#listener_client['dill'] = (create_listener_override(), create_client_override())
listener_client['dill'] = (listener_client['pickle'][0], create_client_override())


class PushManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, serializer="dill", **kwargs)

    def connect(self) -> None:
        super().connect()
        self.register('get_registry')
        for n in self.get_registry().apply():
            self.register(n)
