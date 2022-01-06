class ShowGlobals:
    def apply(self):
        return list(globals().keys())


class ShowLocals:
    def apply(self):
        from boot_common import repl_ts, repl_hosts
        return list(locals().keys())
