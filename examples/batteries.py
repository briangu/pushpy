from __future__ import print_function

import numpy as np
import pandas as pd
from pysyncobj import SyncObjConsumer, replicated


class ReplTimeseries(SyncObjConsumer):
    def __init__(self, on_append=None):
        self.__on_append = on_append
        super(ReplTimeseries, self).__init__()
        self.__data = dict()
        self.__index_data = list()

    @replicated
    def reset(self):
        self.__data = dict()
        self.__index_data = list()

    @replicated
    def append(self, idx_data, keys, data):
        self.__index_data.append(idx_data)
        for key, key_data in zip(keys, data):
            col = self.__data.get(key)
            if col is None:
                col = list()
                self.__data[key] = col
            key_data = key_data if isinstance(key_data, list) else [key_data]
            col.append(key_data)
        if self.__on_append is not None:
            self.__on_append(idx_data, keys, data)

    def flatten(self, keys=None):
        keys = keys or list(self.__data.keys())
        df = pd.DataFrame(columns=keys, index=self.__index_data)
        for key in keys:
            df[key] = np.concatenate(self.__data[key])
        return df