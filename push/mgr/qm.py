from multiprocessing.managers import BaseManager
import dill


class QueueManager(BaseManager):
    def connect(self) -> None:
        super().connect()
        self.register('do_registry')
        for n in self.do_registry().apply():
            self.register(n)
