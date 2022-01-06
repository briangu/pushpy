from multiprocessing.managers import BaseManager, listener_client

import dill


def create_client_override():
    m = listener_client['pickle'][1]

    def __convert(a):
        if isinstance(a, list):
            return [__convert(x) for x in a]
        elif isinstance(a, tuple):
            return tuple(__convert(list(a)))
        elif isinstance(a, dict):
            return {k: __convert(v) for k,v in a}
        return dill.dumps(a) if isinstance(a, type) or callable(a) else a

    def override_client(*args, **kwargs):
        x = m(*args, **kwargs)
        x.__send = x.send

        def _send(*_args, **_kwargs):
            _args = __convert(_args)
            _kwargs = __convert(_kwargs)
            x.__send(*_args, **_kwargs)

        x.send = _send
        return x

    return override_client


listener_client['pickle'] = (listener_client['pickle'][0], create_client_override())


class PushManager(BaseManager):
    def connect(self) -> None:
        super().connect()
        self.register('get_registry')
        for n in self.get_registry().apply():
            self.register(n)
