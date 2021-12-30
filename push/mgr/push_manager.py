from multiprocessing.managers import BaseManager


class PushManager(BaseManager):
    def connect(self) -> None:
        super().connect()
        self.register('get_registry')
        for n in self.get_registry().apply():
            print(n)
            self.register(n)
