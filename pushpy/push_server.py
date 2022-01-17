#!/usr/bin/python3

def main(config_fname=None):
    import asyncio
    import socket
    import time

    import dill
    import tornado.httpserver
    import tornado.web
    from pysyncobj import SyncObj, SyncObjConsumer
    from pysyncobj import SyncObjConf

    from pushpy.batteries import ReplLockDataManager
    from pushpy.code_utils import load_in_memory_module, create_in_memory_module
    from pushpy.host_resources import HostResources, GPUResources, get_cluster_info, get_partition_info
    from pushpy.push_manager import PushManager
    from pushpy.push_server_utils import load_config, serve_forever, host_to_address

    if config_fname is None:
        import sys
        config_fname = sys.argv[1]

    config = load_config(config_fname)
    config_bootstrap = config['bootstrap']
    config_manager = config['manager']
    manager_auth_key = (config_manager.get('auth_key') or 'password').encode('utf8')
    base_host = config.get('hostname') or socket.gethostname()

    if 'manager_host' in config_bootstrap:
        bootstrap_manager_host = config_bootstrap['manager_host']
        print(f"bootstrapping config from {bootstrap_manager_host} {manager_auth_key}")
        bootstrap_manager = PushManager(address=host_to_address(bootstrap_manager_host), authkey=manager_auth_key)
        bootstrap_manager.connect()
        bootstrap_primary = bootstrap_manager.bootstrap_peer()
        peer_config = bootstrap_primary.get_config(base_host, default_base_port=10001)
        sync_obj_port = peer_config['base_port']
        sync_obj_peers = peer_config['sync_obj_config']['peers']
        sync_obj_password = peer_config['sync_obj_config']['password']
        boot_src = dill.loads(peer_config['boot_src'])
        boot_mod, _ = load_in_memory_module(boot_src, name="boot_mod")
    else:
        bootstrap_primary = None
        boot_source_uri = config_bootstrap['boot_source_uri']
        boot_mod, boot_src = load_in_memory_module(boot_source_uri, name="boot_mod")
        config_sync_obj = config['sync_obj']
        sync_obj_port = int(config_sync_obj.get('port') or 10000)
        sync_obj_peers = config_sync_obj.get('peers') or []
        sync_obj_password = config_sync_obj['password'].encode('utf-8') if 'password' in config_sync_obj else None

    manager_port = int(config_manager.get('port') or (sync_obj_port % 1000) + 50000)
    web_port = int((config.get('web') or {}).get('port') or (sync_obj_port % 1000) + 11000)
    sync_obj_host = f"{base_host}:{sync_obj_port}"
    manager_host = f"{base_host}:{manager_port}"
    print(f"sync_obj_host: {sync_obj_host} peers:{sync_obj_peers}")
    print(f"manager_host: {manager_host}")

    class DoRegistry:
        def apply(self):
            return list(PushManager._registry.keys())

    repl_hosts = ReplLockDataManager(autoUnlockTime=5)
    boot_globals, web_router = boot_mod.main()
    boot_consumers = [x for x in boot_globals.values() if isinstance(x, SyncObjConsumer) or hasattr(x, '_consumer')]
    sync_obj = SyncObj(sync_obj_host, sync_obj_peers, consumers=[repl_hosts, *boot_consumers], conf=SyncObjConf(dynamicMembershipChange=True))

    if bootstrap_primary is not None:
        print(f"adding self to cluster {sync_obj_host}")
        bootstrap_primary.apply(sync_obj_host)

    class DoBootstrapPeer:
        def get_host_map(self, hosts):
            print(hosts)
            host_port_map = dict()
            for host in hosts:
                h, p = host.address.split(":")
                arr = host_port_map.get(h)
                if arr is None:
                    arr = []
                    host_port_map[h] = arr
                arr.append(int(p))
            print(host_port_map)
            return host_port_map

        def get_config(self, hostname, default_base_port):
            hosts = [sync_obj.selfNode, *sync_obj.otherNodes]
            host_port_map = self.get_host_map(hosts)
            host_ports = host_port_map.get(hostname, [])
            host_port = max(host_ports) + 1 if default_base_port in host_ports else default_base_port
            return {
                "base_port": host_port,
                "sync_obj_config": {
                    'peers': [x.address for x in hosts],
                    'password': sync_obj_password
                },
                "boot_src": dill.dumps(boot_src)
            }

        def apply(self, peer_address):
            print(f"adding node to cluster: {peer_address}")
            sync_obj.addNodeToCluster(peer_address)

    l_get_cluster_info = lambda: get_cluster_info(repl_hosts)
    l_get_partition_info = lambda: get_partition_info(repl_hosts, sync_obj)

    host_resources = HostResources.create(host_id=sync_obj.selfNode.id, mgr_host=manager_host)
    # override GPU presence if desired
    gpu_count = (((config.get('host_resources') or {}).get('gpu')) or {}).get('count')
    if gpu_count is not None:
        host_resources.gpu = GPUResources(count=gpu_count)

    boot_globals['host_id'] = host_resources.host_id
    boot_globals['get_cluster_info'] = l_get_cluster_info
    boot_globals['get_partition_info'] = l_get_partition_info
    boot_globals['host_resources'] = host_resources

    PushManager.register('sync_obj', callable=lambda: sync_obj)
    PushManager.register('bootstrap_peer', callable=lambda: DoBootstrapPeer())
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
    print(f"bind complete: {sync_obj.selfNode.id}")
    while not repl_hosts.tryAcquire(sync_obj.selfNode.id, data=host_resources, sync=True):
        print(f"connecting to cluster...")
        time.sleep(0.1)

    m = PushManager(address=host_to_address(manager_host), authkey=manager_auth_key)
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
