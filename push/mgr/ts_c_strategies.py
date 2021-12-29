import sys

import numpy as np

from push.mgr.qm import QueueManager
from push.mgr.strategy import Strategy

m = QueueManager(address=('', int(sys.argv[1])), authkey=b'password')
m.connect()

symbols = ['MSFT', 'TWTR', 'EBAY', 'CVX', 'W', 'GOOG', 'FB']
strategy_capabilities = ['CPU', 'GPU']
np.random.seed(0)
strategies = [Strategy(id=i, name=f"s_{i}", symbols=np.random.choice(symbols, 2), capabilities=np.random.choice(strategy_capabilities, 1)) for i in range(10)]

repl_strategies = m.strategies()
repl_strategies.reset(strategies)
