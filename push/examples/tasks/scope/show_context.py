class ShowGlobals:
    def apply(self):
        return list(globals().keys())


class ShowLocals:
    def apply(self):
        # show that we can use boot_common to import some instances
        from boot_common import repl_ts, repl_ver_store
        return list(locals().keys())
