from datetime import datetime
import os
import re
import shlex


def launch_command(python=None, worker_cls=None, scheduler=None, memory=0, worker_name=None, timeout=None, 
                  interface=None, tls_ca_file=None, tls_cert=None, tls_key=None, worker_port=None, nanny=True, 
                  listen_address=None, contact_address=None, nworkers=None, nthreads=None, resources=None,
                  nanny_port=None, dashboard=True, protocol=None, pid_file=None, local_dir=None, scheduler_file=None,
                  dashboard_prefix=None):
    command = []
    if not python:
        python = "python3"        
    command.append(python)
    if not worker_cls:
        worker_cls = "distributed.cli.dask_worker"
    command += ["-m", worker_cls]
    if not scheduler:
        raise ValueError("You must specify the scheduler")
    command.append(scheduler)
    if not nthreads:
        nthreads = 1
    command += ["--nthreads", nthreads]
    if not nworkers:
        raise ValueError("You must specify the number of workers")
    command += ["--nworkers", nworkers]
    if memory:
       command += ["--memory-limit", memory]
    if worker_name:
        command += ["--name", worker_name]
    if timeout:
        command += ["--death-timeout", timeout]
    if nanny:
        command += ["--nanny"]
    if not interface:
        raise ValueError("You must specify an interface")
    command += ["--interface", interface]
    if nanny_port:
        command += ["--nanny-port", nanny_port]
    if resources:
        command += ["--resources", resources]
    if tls_ca_file:
        command += ["--tls-ca-file", tls_ca_file]
    if tls_cert:
        command += ["--tls-cert", tls_cert]
    if tls_key:
        command += ["--tls-key", tls_key]
    if worker_port:
        command += ["--worker-port", worker_port]
    if listen_address:
        command += ["--listen-address", listen_address]
    if contact_address:
        command += ["--contact-address", listen_address]
    if not dashboard:
        command.append("--no-dashboard")
    if protocol:
        command += ["--protocol", protocol]
    if pid_file:
        command += ["--pid-file", pid_file]
    if local_dir:
        command += ["--local-directory", local_dir]
    if scheduler_file:
        command += ["--scheduler-file", scheduler_file]
    if dashboard_prefix:
        command += ["--dashboard-prefix", dashboard_prefix]
    return " ".join(map(str, command))


def salloc_command(account=None, chdir=None, clusters=None, exclusive=True, gpus=None, name=None, memory=None, 
                   nodes=None, partition=None, time=None, extras=None):
    command = ["salloc"]
    if account:
        command += ["-A", account]
    if chdir:
        command += ["-D", chdir]
    if clusters:
        command += ["-M", clusters]
    if exclusive:
        command.append("--exclusive")
    if gpus:
        command += ["-G", gpus]
    if name:
        command += ["-J", name]
    if memory:
        command += ["--mem", memory]
    if nodes:
        command += ["-N", nodes]
    if partition:
        command += ["-p", partition]
    if time:
        command += ["-t", time]
    if extras:
        command += extras
    command.append("--no-shell")
    return command

def memory_command():
    command = "free -g | grep 'Mem' | sed 's/[\t ][\t ]*/ /g' | cut -d ' ' -f 7"
    return shlex.split(command, posix=False)

def core_command():
    return ["nproc", "--all"]


# Handle what to do if name is null or invalid
def jobid_command(name):
    command = f"squeue --name={name} -o %i | tail -n 1"
    return shlex.split(command, posix=False)

def nodelist_command(name):
    command = f"squeue --name={name} -o %N | tail -n 1"
    return shlex.split(command, posix=False)

def jobcheck_command(jobid):
    command = f"squeue -j {jobid} -o %i | tail -n 1"
    return shlex.split(command, posix=False)

def parse_nodelist(nodelist):
    nodes = []
    matched = re.search(r'\[(.*)\]', nodelist)
    if matched:
        prefix = nodelist[:matched.start()]
        elements = matched.group(1).split(',')
        for element in elements:
            index = element.find('-')
            if index != -1:
                start_node = element[:index].strip() 
                end_node = element[(index + 1):].strip()
                padding_len = len(start_node)
                start = int(start_node)
                end = int(end_node)
                while start <= end:
                    node = prefix + str(start).zfill(padding_len)
                    nodes.append(node)
                    start += 1
            else:
                nodes.append(prefix + str(element.strip()))
    else:
        nodes.append(nodelist)
    return nodes

def create_logs_folder(cluster_name):
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y%m%d_%H%M%S")
    folder_name = f"{cluster_name}_{formatted_datetime}_logs"
    folder_path = os.path.join(os.path.expanduser('~'), folder_name)
    os.makedirs(folder_path)
    return folder_path
