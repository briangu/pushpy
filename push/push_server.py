from pysyncobj import SyncObjConf


def main(config_fname):
    import asyncio
    import socket
    import time

    import tornado.httpserver
    import tornado.web
    from pysyncobj import SyncObj, SyncObjConsumer

    from push.batteries import ReplLockDataManager
    from push.code_utils import load_in_memory_module, create_in_memory_module
    from push.host_resources import HostResources, GPUResources, get_cluster_info, get_partition_info
    from push.push_manager import PushManager
    from push.push_server_utils import load_config, serve_forever, host_to_address

    config = load_config(config_fname)
    boot_module_src = config['boot_source_uri']
    gpu_count = (((config.get('host_resources') or {}).get('gpu')) or {}).get('count')
    base_host = config.get('hostname') or 'localhost' #socket.gethostname()
    sync_obj_port = int(config['sync_obj']['port'])
    sync_obj_host = f"{base_host}:{sync_obj_port}"
    mgr_port = int(config['manager'].get('port') or (sync_obj_port % 1000) + 50000)
    mgr_host = f"{base_host}:{mgr_port}"
    mgr_auth_key = (config['manager'].get('auth_key') or 'password').encode('utf8')
    sync_obj_peers = config['sync_obj'].get('peers') or []
    sync_obj_password = config['sync_obj']['password'].encode('utf-8') if 'password' in config['sync_obj'] else None
    web_port = int((config.get('web') or {}).get('port') or (sync_obj_port % 1000) + 11000)

    class DoRegistry:
        def apply(self):
            return list(PushManager._registry.keys())

    repl_hosts = ReplLockDataManager(autoUnlockTime=5)
    boot_mod = load_in_memory_module(boot_module_src, name="boot_mod")
    boot_globals, web_router = boot_mod.main()
    boot_consumers = [x for x in boot_globals.values() if isinstance(x, SyncObjConsumer) or hasattr(x, '_consumer')]
    sync_obj_config = SyncObjConf(password=sync_obj_password, dynamicMembershipChange=True)
    sync_obj = SyncObj(sync_obj_host, sync_obj_peers, consumers=[repl_hosts, *boot_consumers], conf=sync_obj_config)

    l_get_cluster_info = lambda: get_cluster_info(repl_hosts)
    l_get_partition_info = lambda: get_partition_info(repl_hosts, sync_obj)

    host_resources = HostResources.create(host_id=sync_obj.selfNode.id, mgr_host=mgr_host)
    # override GPU presence if desired
    if gpu_count is not None:
        host_resources.gpu = GPUResources(count=gpu_count)

    boot_globals['host_id'] = host_resources.host_id
    boot_globals['get_cluster_info'] = l_get_cluster_info
    boot_globals['get_partition_info'] = l_get_partition_info
    boot_globals['host_resources'] = host_resources

    PushManager.register('sync_obj', callable=lambda: sync_obj)
    PushManager.register('get_registry', callable=lambda: DoRegistry())
    PushManager.register("get_cluster_info", callable=lambda: l_get_cluster_info)
    PushManager.register("get_partition_info", callable=lambda: l_get_partition_info)
    PushManager.register("host_resources", callable=lambda: host_resources)

    boot_common = create_in_memory_module(name="boot_common")

    for k, v in boot_globals.items():
        globals()[k] = v
        boot_common.__dict__[k] = v
        if k.startswith("repl_") or k.startswith("local_"):
            # https://stackoverflow.com/questions/2295290/what-do-lambda-function-closures-capture
            PushManager.register(k, callable=lambda q=k: globals()[q])

    print(f"registering host: {sync_obj.selfNode.id}")
    sync_obj.waitReady()
    while not repl_hosts.tryAcquire(sync_obj.selfNode.id, data=host_resources, sync=True):
        print(f"connecting to cluster...")
        time.sleep(0.1)

    m = PushManager(address=host_to_address(mgr_host), authkey=mgr_auth_key)
    mgmt_server = m.get_server()
    mt = serve_forever(mgmt_server)

    if web_router is None:
        mt.join()
    else:
        webserver = tornado.httpserver.HTTPServer(web_router)
        print(f"starting webserver @ {web_port}")
        webserver.listen(web_port)

        # use asyncio to drive tornado so that async io can be used in web handlers
        loop = asyncio.get_event_loop()

        try:
            loop.run_forever()
        finally:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()

    print(f"stopping")


if __name__ == "__main__":
    import sys

    main(sys.argv[1])
