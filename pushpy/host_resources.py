import typing

import GPUtil
import psutil


class Resource:

    def __str__(self):
        return str(vars(self))

    def __repr__(self):
        return self.__str__()

    def update(self):
        pass

    def has_capacity(self, requirement):
        pass

    def is_compatible(self, other):
        return True


class MemoryRequirements(Resource):

    total: int

    def __init__(self, total):
        self.total = total


class MemoryResources(Resource):
    total: int
    available: int

    def __init__(self, total, available):
        self.total = total
        self.available = available

    def update(self):
        vm = psutil.virtual_memory()
        self.available = vm.available
        return self

    def has_capacity(self, requirement: MemoryRequirements):
        return self.available >= requirement.total

    @staticmethod
    def create():
        vm = psutil.virtual_memory()
        return MemoryResources(vm.total, vm.available)


class CPURequirements(Resource):

    count: int

    def __init__(self, count):
        self.count = count


class CPUResources(Resource):
    count: int
    available: int

    def __init__(self, count, available):
        self.count = count
        self.available = available

    def update(self):
        self.available = int(((100 - psutil.cpu_percent()) / 100) * self.count)
        return self

    def has_capacity(self, requirement: CPURequirements):
        return self.available >= requirement.count

    @staticmethod
    def create():
        count = psutil.cpu_count()
        available = int((100 - psutil.cpu_percent()) * count)
        return CPUResources(count=count, available=available)


class GPURequirements(Resource):

    count: int
    # TODO: include memory

    def __init__(self, count):
        self.count = count


class GPUResources(Resource):
    count: int
    # TODO: include memory
    available: int

    def __init__(self, count):
        self.count = count
        self.available = count

    def update(self):
        # TODO update based on load
        pass

    def has_capacity(self, requirement: GPURequirements):
        return self.available >= requirement.count if requirement.count > 0 else self.available == 0

    def is_compatible(self, other):
        return (self.count > 0 and other.count > 0) or (self.count == other.count)

    @staticmethod
    def create():
        gpus = GPUtil.getGPUs()
        return GPUResources(count=len(gpus))


class ManagerResources(Resource):
    host: str

    def __init__(self, host):
        self.host = host

    def __str__(self):
        return str(vars(self))

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def create(host):
        return ManagerResources(host)


class HostRequirements(Resource):

    cpu: typing.Optional[CPURequirements]
    memory: typing.Optional[MemoryRequirements]
    gpu: typing.Optional[GPURequirements]

    def __init__(self,
                 cpu: typing.Optional[CPURequirements],
                 memory: typing.Optional[MemoryRequirements],
                 gpu: typing.Optional[GPURequirements]):
        self.cpu = cpu
        self.memory = memory
        self.gpu = gpu


class HostResources(Resource):
    host_id: str
    cpu: CPUResources
    memory: MemoryResources
    gpu: GPUResources
    mgr: ManagerResources

    def __init__(self,
                 host_id: str,
                 cpu: CPUResources,
                 memory: MemoryResources,
                 gpu: GPUResources,
                 mgr: ManagerResources):
        self.host_id = host_id
        self.cpu = cpu
        self.memory = memory
        self.gpu = gpu
        self.mgr = mgr

    def update(self):
        self.cpu.update()
        self.memory.update()
        self.gpu.update()
        return self

    def is_compatible(self, other):
        return self.cpu.is_compatible(other.cpu) and \
               self.memory.is_compatible(other.memory) and \
               self.gpu.is_compatible(other.gpu)

    def has_capacity(self, requirement: HostRequirements):
        return (self.cpu.has_capacity(requirement.cpu) if requirement.cpu is not None else True) and \
               (self.memory.has_capacity(requirement.memory) if requirement.memory is not None else True) and \
               (self.gpu.has_capacity(requirement.gpu) if requirement.gpu is not None else True)

    @staticmethod
    def create(host_id, mgr_host=None):
        return HostResources(
            host_id=host_id,
            cpu=CPUResources.create(),
            memory=MemoryResources.create(),
            gpu=GPUResources.create(),
            mgr=ManagerResources.create(mgr_host)
        )


def get_cluster_info(hosts):
    return hosts.lockData()


def get_partition_info(hosts, so):
    all_nodes = [so.selfNode, *so.otherNodes]
    all_host_resources = hosts.lockData()
    if so.selfNode.id not in all_host_resources:
        return 0, 0, {}
    host_resources = all_host_resources[so.selfNode.id]
    all_nodes = [x for x in all_nodes if hosts.isOwned(x.id)]
    if so.selfNode not in all_nodes:
        return 0, 0, {}
    all_nodes = sorted(all_nodes, key=lambda x: x.id)
    all_nodes = [x for x in all_nodes if host_resources.is_compatible(all_host_resources[x.id])]
    return len(all_nodes), all_nodes.index(so.selfNode), host_resources
