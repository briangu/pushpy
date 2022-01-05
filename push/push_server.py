def main():
    import asyncio
    import sys
    import time

    import tornado.httpserver
    import tornado.web
    from pysyncobj import SyncObj, SyncObjConsumer

    from push.loader import load_in_memory_module
    from push.batteries import ReplLockDataManager
    from push.host_resources import HostResources, GPUResources, get_cluster_info, get_partition_info
    from push.push_manager import PushManager
    from push.push_util import serve_forever

    boot_module_src = sys.argv[1]
    gpu_capabilities = sys.argv[2]
    selfAddr = sys.argv[3]
    base_port = int(selfAddr.split(":")[1])
    mgr_port = (base_port % 1000) + 50000
    partners = sys.argv[4:]

    # fake GPU for testing
    host_resources = HostResources.create(mgr_port=mgr_port)
    host_resources.gpu = GPUResources(count=1 if 'GPU' in gpu_capabilities else 0)

    class DoRegistry:
        def apply(self):
            return list(PushManager._registry.keys())

    PushManager.register('get_registry', callable=lambda: DoRegistry())
    PushManager.register('host_resources', callable=lambda: host_resources)

    # >>> setup sync obj
    repl_hosts = ReplLockDataManager(autoUnlockTime=5)
    boot_mod = load_in_memory_module(boot_module_src, name="boot_common")
    boot_globals, web_router = boot_mod.main()
    boot_consumers = [x for x in boot_globals.values() if isinstance(x, SyncObjConsumer) or hasattr(x, '_consumer')]
    sync_obj = SyncObj(selfAddr, partners, consumers=[repl_hosts, *boot_consumers])

    PushManager.register('sync_obj', callable=lambda: sync_obj)

    for k, v in boot_globals.items():
        globals()[k] = v
        if k.startswith("repl_") or k.startswith("local_"):
            # https://stackoverflow.com/questions/2295290/what-do-lambda-function-closures-capture
            PushManager.register(k, callable=lambda q=k: globals()[q])

    print(f"setting up resources")
    l_get_cluster_info = lambda: get_cluster_info(repl_hosts)
    l_get_partition_info = lambda: get_partition_info(repl_hosts, sync_obj)

    globals()['get_cluster_info'] = l_get_cluster_info
    globals()['get_partition_info'] = l_get_partition_info
    globals()['host_resources'] = host_resources

    # update the boot_common module with host info
    mod = sys.modules['boot_common']
    mod.__dict__['get_cluster_info'] = l_get_cluster_info
    mod.__dict__['get_partition_info'] = l_get_partition_info
    mod.__dict__['host_resources'] = host_resources

    PushManager.register("get_cluster_info", callable=lambda: l_get_cluster_info)
    PushManager.register("get_partition_info", callable=lambda: l_get_partition_info)
    PushManager.register("host_resources", callable=lambda: host_resources)

    print(f"registering host: {sync_obj.selfNode.id}")
    sync_obj.waitReady()
    while not repl_hosts.tryAcquire(sync_obj.selfNode.id, data=host_resources, sync=True):
        print(f"connecting to cluster...")
        time.sleep(0.1)

    # <<< setup sync obj

    if web_router is not None:
        webserver = tornado.httpserver.HTTPServer(web_router)
        web_port = (base_port % 1000) + 11000
        print(f"starting webserver @ {web_port}")
        webserver.listen(web_port)

    serve_forever(mgr_port, b'password')

    # tornado.ioloop.IOLoop.current().start()
    loop = asyncio.get_event_loop()
    try:
        loop.run_forever()
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
        print(f"stopping")


if __name__ == "__main__":
    main()
