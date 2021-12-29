import typing


class Strategy:

    def __init__(self,
                 id: int,
                 name: str,
                 symbols: typing.List[str],
                 capabilities: typing.Optional[typing.List[str]] = None):
        self.id = id
        self.name = name
        self.symbols = symbols
        self.capabilities = capabilities

    def __hash__(self):
        return hash(int(self.id))

    def __str__(self):
        return str(vars(self))

    def __repr__(self):
        return self.__str__()
