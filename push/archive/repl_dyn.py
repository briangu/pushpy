# # TODO: scan repl_obj for all methods and add to this, proxying all replicated operations to parent
# class _ReplDynamicProxy:
#     def __init__(self, parent, name, repl_obj):
#         self.parent = parent
#         self.name = name
#         self.repl_obj = repl_obj
#
#     def apply(self, method, *args, **kwargs):
#         return self.parent.apply(self.name, method, *args, **kwargs)
#
#
# # usage: this will be the base repl data structure for a Push server
# #       it supports adding new / removing named sub-consumers as a replicated action
# #       it supports operating on added sub-consumers as a replicated action
# class ReplDynamicConsumer(SyncObjConsumer):
#     def __init__(self):
#         super(ReplDynamicConsumer, self).__init__()
#         self.__properties = set()
#         for key in self.__dict__:
#             self.__properties.add(key)
#         self.__data = {}
#
#     def obj_from_type(self, repl_type):
#         if repl_type == "list":
#             obj = ReplList()
#         elif repl_type == "dict":
#             obj = ReplDict()
#         elif repl_type == "ts":
#             obj = ReplTimeseries()
#         else:
#             raise RuntimeError(f"unknown type: {repl_type}")
#         obj._syncObj = self
#         return obj
#
#     @replicated
#     def add(self, name, repl_type):
#         if name in self.__data:
#             raise RuntimeError(f"name already present: {name}")
#         self.__data[name] = {'type': repl_type, 'obj': self.obj_from_type(repl_type)}
#
#     @replicated
#     def remove(self, name):
#         self.__delitem__(name, _doApply=True)
#
#     @replicated
#     def __delitem__(self, name):
#         if name in self.__data:
#             del self.__data[name]
#
#     @replicated
#     def apply(self, name, method, *args, **kwargs):
#         if name not in self.__data:
#             raise RuntimeError(f"name already present: {name}")
#         d = self.__data[name]['obj']
#         if not hasattr(d, method):
#             raise RuntimeError(f"method not found: {name} {method}")
#         return getattr(d, method)(*args, **kwargs)
#
#     def __getitem__(self, name):
#         repl_obj = self.__data.get(name)['obj']
#         return _ReplDynamicProxy(self, name, repl_obj) if repl_obj is not None else None
#
#     def _serialize(self):
#         d = dict()
#         for k, v in [(k, v) for k, v in iteritems(self.__dict__) if k not in self.__properties]:
#             if k.endswith("__data") and isinstance(v, dict):
#                 _d = dict()
#                 for _k, _v in iteritems(v):
#                     __d = dict()
#                     __d['type'] = _v['type']
#                     __d['obj'] = _v['obj']._serialize()
#                     _d[_k] = __d
#                 v = _d
#             d[k] = v
#         return d
#
#     # TODO: recurse into subconsumers
#     def _deserialize(self, data):
#         for k, v in iteritems(data):
#             if k.endswith("__data") and isinstance(v, dict):
#                 _d = dict()
#                 for _k, _v in iteritems(v):
#                     __d = dict()
#                     __d['type'] = _v['type']
#                     obj = self.obj_from_type(_v['type'])
#                     obj._deserialize(_v['obj'])
#                     __d['obj'] = obj
#                     _d[_k] = __d
#                 v = _d
#             self.__dict__[k] = v
