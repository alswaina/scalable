import subprocess
import yaml
import os
import asyncio
from dask.utils import parse_bytes
import re

comm_port_regex = r'0\.0\.0\.0:(\d{1,5})'

def send_command(command, port, communicator_path=None):
    if communicator_path is None:
        communicator_path = "./communicator"
    if not os.path.isfile(communicator_path):
        raise FileNotFoundError("The communicator file does not exist at the given path" +
                                "(default current directory). Please try again.")
    communicator_command = []
    communicator_command.append(communicator_path)
    communicator_command.append("-c")
    communicator_command.append(str(port))
    command += "\n"
    process = subprocess.Popen(args=communicator_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    input = bytes(command, encoding='utf-8')
    out, _ = process.communicate(input=input)
    result = str(out, encoding='utf-8')
    result = result.strip()
    return result

async def get_cmd_comm(port, communicator_path=None):
    if communicator_path is None:
        communicator_path = "./communicator"
    if not os.path.isfile(communicator_path):
        raise FileNotFoundError("The communicator file does not exist at the given path" +
                                "(default current directory). Please try again.")
    communicator_command = []
    communicator_command.append(communicator_path)
    communicator_command.append("-c")
    communicator_command.append(str(port))
    proc = await asyncio.create_subprocess_exec(
        *communicator_command,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
    )
    return proc

def get_comm_port(logpath=None):
    if logpath is None:
        logpath = "./communicator.log"
    ret = -1
    with open(logpath, 'r') as file:
        for line in file:
            match = re.search(comm_port_regex, line)
            if match:
                port = int(match.group(1))
                if 0 <= port <= 65535:
                    ret = port
                    break
    return ret
        
def make_resource_dict():
    if not os.path.isfile('resource_list.yaml'):
        open('resource_list.yaml', 'a').close()
    return True

def add_extras(**kwargs):
    ret = True
    if os.path.isfile('resource_list.yaml'):
        with open('resource_list.yaml', 'r') as file:
            resource_dict = yaml.safe_load(file)
            if resource_dict is None:
                resource_dict = {}
            for k, v in kwargs.items():
                resource_dict[k] = v
            with open('resource_list.yaml', 'w') as file:
                yaml.dump(resource_dict, file, default_flow_style=False)
    else:
        ret = False
    return ret

def set_container_runtime(runtime):
    ret = True
    if os.path.isfile('resource_list.yaml'):
        with open('resource_list.yaml', 'r') as file:
            resource_dict = yaml.safe_load(file)
            if resource_dict is None:
                resource_dict = {}
            resource_dict["Runtime"] = runtime
            with open('resource_list.yaml', 'w') as file:
                yaml.dump(resource_dict, file, default_flow_style=False)
    else:
        ret = False
    return ret

# LOG ERRORS WHEN RESOURCE DICT IS NOT FOUND

def add_resource(model, cpus=None, memory=None, path=None, extras=None):
    ret = True
    insert = {}
    if cpus:
        insert["CPUs"] = cpus
    if memory:
        memory_parsed = parse_bytes(memory)
        memory_parsed //= 10**9
        insert["Memory"] = memory_parsed
    if path:
        insert["Path"] = path
    if extras:
        insert.update(extras)
    if os.path.isfile('resource_list.yaml'):
        with open('resource_list.yaml', 'r') as file:
            resource_dict = yaml.safe_load(file)
            if resource_dict is None:
                resource_dict = {}
            if model in resource_dict.keys():
                resource_dict[model].update(insert)
            else:
                resource_dict[model] = insert
        with open('resource_list.yaml', 'w') as file:
            yaml.dump(resource_dict, file, default_flow_style=False)
    else:
        ret = False
    return ret

def delete_resource_dict():
    if os.path.isfile('resource_list.yaml'):
        os.remove('resource_list.yaml')
    return True

def get_resource_dict():
    ret = None
    if os.path.isfile('resource_list.yaml'):
        with open('resource_list.yaml', 'r') as file:
            resource_dict = yaml.safe_load(file)
            ret = resource_dict
    return ret


class HardwareResources:

    MIN_CPUS = 1
    MIN_MEMORY = 2

    def __init__(self):
        self.nodes = []
        self.assigned = {}
        self.available = {}
        self.active = {}

    def assign_resources(self, node, cpus, memory, jobid):
        allotted = {'cpus': cpus, 'memory': memory, 'jobid': jobid}
        if node not in self.assigned and node not in self.available:
            self.assigned[node] = allotted
            self.available[node] = allotted.copy()
            self.nodes.append(node)
            if jobid not in self.active:
                self.active[jobid] = set()
        else:
            raise ValueError(
                "The node already exists. New resources to an existing node \
                cannot be assigned. Please try again.\n"
            )
    
    def remove_jobid_nodes(self, jobid):
        nodes = self.nodes
        if jobid in self.active:
            del self.active[jobid]
        delete = []
        for node in nodes:
            if self.assigned[node]['jobid'] == jobid:
                del self.assigned[node]
                del self.available[node]
                delete.append(node)
        for node in delete:
            self.nodes.remove(node)
    
    def get_node_jobid(self, node):
        if node not in self.available:
            raise ValueError(
                "The given node doesn't exist. Please try again.\n"
            )
        else:
            return self.available[node]['jobid']

    def check_availability(self, cpus, memory):
        ret = None
        for node, specs in self.available.items():
            if (specs['cpus'] - cpus) > self.MIN_CPUS and (specs['memory'] - memory) > self.MIN_MEMORY:
                ret = node
                break
        return ret

    def utilize_resources(self, node, cpus, memory, jobid):
        if node not in self.available or self.available[node]['jobid'] != jobid:
            raise ValueError (
                "There are not enough hardware resources available. Please \
                allocate more hardware resources and try again.\n"
            )
        self.available[node]['cpus'] -= cpus
        self.available[node]['memory'] -= memory
        self.active[self.available[node]['jobid']].add(node)

    def release_resources(self, node, cpus, memory, jobid):
        if node in self.available and node in self.assigned:
            if self.available[node]['jobid'] != jobid:
                return
            self.available[node]['cpus'] += cpus
            self.available[node]['memory'] += memory
        if self.available[node]['cpus'] ==  self.assigned[node]['cpus'] and \
        self.available[node]['memory'] == self.assigned[node]['memory']:
            self.active[self.available[node]['jobid']].remove(node)
    
    def has_active_nodes(self, jobid):
        ret = True
        if jobid not in self.active or len(self.active[jobid]) == 0:
            ret = False
        return ret
    
    @staticmethod
    def set_min_free_cpus(cpus):
        HardwareResources.MIN_CPUS = cpus

    @staticmethod
    def set_min_free_memory(memory):
        HardwareResources.MIN_MEMORY = memory

class Container:

    _runtime_directives = {"apptainer": "exec", "docker": "run"}

    _runtime = "apptainer"

    def __init__(self, name, cpus, memory, path, directories=None) -> None:
        self.name = name
        self.cpus = cpus
        memory_parsed = parse_bytes(memory)
        memory_parsed //= 10**9
        self.memory = memory_parsed
        self.path = path
        if  directories is None:
            directories = {}
        self.directories = directories

    def add_directory(self, src, dst=None):
        if dst is None:
            dst = src
        self.directories[src] = dst

    def get_info_dict(self):
        ret = {}
        ret['Name'] = self.name
        ret['CPUs'] = self.cpus
        ret['Memory'] = self.memory
        ret['Path'] = self.path
        ret['Directories'] = self.directories
        return ret

    def get_command(self):
        command = []
        command.append(Container.get_runtime())
        command.append(Container.get_runtime__directive())
        for src, dst in self.directories.items():
            if dst is None or dst == "":
                dst = src
            command.append("--bind")
            command.append(f"{src}:{dst}")
        command.append(self.path)
        return command

    @staticmethod
    def get_runtime():
        if Container._runtime is None or "":
            raise ValueError(
                "Runtime has not been set. Please set it using set_runtime()."
            )
        return Container._runtime

    @staticmethod
    def get_runtime__directive():
        if Container._runtime not in Container._runtime_directives:
            raise ValueError(
                "Runtime has not been set. Please set it using \
                set_runtime_directive()s."
            )
        return Container._runtime_directives[Container._runtime]
    
    @staticmethod
    def set_runtime(runtime):
        Container._runtime = runtime

    @staticmethod
    def set_runtime_directive(runtime, directive):
        Container._runtime_directives[runtime] = directive