#!/usr/bin/env python
# coding=utf-8
#
# Copyright (c) 2018 Excelero, Inc. All rights reserved.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# Author:        Andreas Krause
# Maintainer:    Andreas Krause
# Email:         andreas@excelero.com

from cmd2 import Cmd, with_argparser
import argparse
import json
import gnureadline as readline
import atexit
import sys
import os
import getpass
import paramiko
import base64
from humanfriendly.tables import format_smart_table
import humanfriendly
from constants import *
import nvmesh_api
import time
import urllib3
from multiprocessing import Pool


class OutputFormatter:
    def __init__(self):
        self.text = None

    @staticmethod
    def print_green(text):
        print '\033[92m' + text + '\033[0m'

    @staticmethod
    def print_yellow(text):
        print '\033[33m' + text + '\033[0m'

    @staticmethod
    def print_red(text):
        print '\033[31m' + text + '\033[0m'

    @staticmethod
    def green(text):
        return '\033[92m' + text + '\033[0m'

    @staticmethod
    def yellow(text):
        return '\033[33m' + text + '\033[0m'

    @staticmethod
    def red(text):
        return '\033[31m' + text + '\033[0m'

    @staticmethod
    def bold(text):
        return '\033[1m' + text + '\033[0m'

    @staticmethod
    def bold_underline(text):
        return '\033[1m\033[4m' + text + '\033[0m'

    @staticmethod
    def echo(host, text):
        print "[ " + host.strip() + " ]\t.\t.\t." + text
        return

    @staticmethod
    def print_tsv(content):
        output = []
        for line in content:
            output_line = "\t".join(str(item) for item in line)
            output.append(output_line)
        return "\n".join(output)

    @staticmethod
    def print_json(content):
        return json.dumps(content, indent=2)

    @staticmethod
    def add_line_prefix(prefix, text):
        text_lines = [' '.join([prefix.split('.')[0], line]) for line in text.splitlines()]
        return '\n'.join(text_lines) + "\n"


class Hosts:
    def __init__(self):
        self.host_list = []
        self.host_file = os.path.expanduser('~/.nvmesh_hosts')
        self.test_host_connection_test_result = None
        self.formatter = OutputFormatter()
        self.host_delete_list = []

    def manage_hosts(self, action, hosts_list):
        if action == "add":
            open(self.host_file, 'a').write(('\n'.join(hosts_list) + '\n'))
        elif action == "list":
            if os.path.isfile(self.host_file):
                self.host_list = open(self.host_file, 'r').readlines()
                print ("Found the following hosts/servers configured:")
                for host in self.host_list:
                    print(host.strip())
            else:
                print(self.formatter.yellow("No hosts/servers defined! Use 'add hosts' to add servers to your"
                                            " shell environment."))
        elif action == "delete":
            tmp_host_list = []
            if os.path.isfile(self.host_file):
                for line in open(self.host_file, 'r').readlines():
                    tmp_host_list.append(line.strip())
                for host in hosts_list:
                    tmp_host_list.remove(host.strip())
                open(self.host_file, 'w').write(('\n'.join(tmp_host_list) + '\n'))
            else:
                print(self.formatter.print_yellow("No hosts/servers defined!"))


class ManagementServer:
    def __init__(self):
        self.server = None
        self.server_list = []
        self.server_file = os.path.expanduser('~/.nvmesh_manager')

    def get_management_server(self):
        if os.path.isfile(self.server_file):
            self.server = open(self.server_file, 'r').read()
            return self.server
        else:
            formatter.print_yellow("No API management server defined yet!")
            self.server = raw_input("Provide the NVMesh manager server name: ")
            self.save_management_server([self.server])
            return None

    def save_management_server(self, manager):
        open(self.server_file, 'w').write(manager[0])
        return

    def get_management_server_list(self):
        if os.path.isfile(self.server_file):
            self.server_list = open(self.server_file, 'r').readlines()
            return self.server_list
        else:
            formatter.print_yellow("No API management server defined yet! Run 'define manager' first!")
            return None


class UserCredentials:
    def __init__(self):
        self.SSH_user_name = None
        self.SSH_password = None
        self.API_user_name = None
        self.API_password = None
        self.SSH_secrets_file = os.path.expanduser('~/.nvmesh_shell_secrets')
        self.API_secrets_file = os.path.expanduser('~/.nvmesh_api_secrets')
        self.secrets = None

    def save_ssh_user(self):
        if self.SSH_user_name is None or self.SSH_password is None:
            formatter.print_red('Cannot store SSH user credentials! '
                                'Both, user name and password need to be set/defined!')
        else:
            secrets = open(self.SSH_secrets_file, 'w')
            secrets.write(' '.join([self.SSH_user_name, base64.b64encode(self.SSH_password)]))
            secrets.close()

    def save_api_user(self):
        if self.API_user_name is None or self.API_password is None:
            formatter.print_red('Cannot store API user credentials! '
                                'Both, user name and password need to be set/defined!')
        else:
            secrets = open(self.API_secrets_file, 'w')
            secrets.write(' '.join([self.API_user_name, base64.b64encode(self.API_password)]))
            secrets.close()

    def get_ssh_user(self):
        try:
            self.secrets = open(self.SSH_secrets_file, 'r').read().split(' ')
        except Exception, e:
            pass
        if self.secrets is None:
            formatter.print_yellow("SSH user credentials not set yet!")
            self.SSH_user_name = raw_input("Provide the root level SSH user name: ")
            self.SSH_password = getpass.getpass("Please provide the SSH password: ")
            self.save_ssh_user()
            return self.SSH_user_name
        else:
            self.SSH_user_name = self.secrets[0]
            self.SSH_password = base64.b64decode(self.secrets[1])
            return self.SSH_user_name

    def get_api_user(self):
        try:
            self.secrets = open(self.API_secrets_file, 'r').read().split(' ')
        except Exception, e:
            pass
        if self.secrets is None:
            formatter.print_yellow("API user credentials not set yet!")
            self.API_user_name = raw_input("Provide the root level API user name: ")
            self.API_password = getpass.getpass("Please provide the API password: ")
            self.save_api_user()
            return self.API_user_name
        else:
            self.API_user_name = self.secrets[0]
            self.API_password = base64.b64decode(self.secrets[1])
            return self.API_user_name


class SSHRemoteOperations:
    def __init__(self):
        self.remote_path = "/tmp/nvmesh_diag/"
        self.local_path = os.path.abspath("nvmesh_diag/")
        self.formatter = OutputFormatter
        self.file_list = []
        self.ssh_port = 22
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.sftp = self.ssh.open_sftp
        self.remote_return_code = None
        self.remote_stdout = None
        self.remote_command_return = None
        self.remote_command_error = None

    def test_ssh_connection(self, host, username, password):
        try:
            self.ssh.connect(host, username=username, password=password, timeout=5, port=self.ssh_port)
            print formatter.print_green("Connection to host %s OK" % host)
        except Exception, e:
            print formatter.print_red("Connection to host %s Failed! " % host + e.message)
        self.ssh.close()
        return

    def transfer_files(self, host, username, password, list_of_files):
        try:
            self.ssh.connect(host, username=username, password=password, timeout=5, port=self.ssh_port)
            self.sftp = self.ssh.open_sftp()
            try:
                self.sftp.chdir(self.remote_path)
            except IOError:
                self.sftp.mkdir(self.remote_path)
            for file_to_transfer in list_of_files:
                self.sftp.put(self.local_path + "/" + file_to_transfer, self.remote_path + "/" + file_to_transfer)
            self.sftp.close()
            print formatter.print_green("File transfer to host %s OK" % host)
        except Exception, e:
            print formatter.print_red("File transfer to %s Failed! " % host + e.message)
        self.ssh.close()

    def return_remote_command_std_output(self, host, remote_command):
        try:
            self.ssh.connect(host, username=user.SSH_user_name, password=user.SSH_password, timeout=5,
                             port=self.ssh_port)
            stdin, stdout, stderr = self.ssh.exec_command(remote_command)
            self.remote_command_return = stdout.channel.recv_exit_status(), stdout.read().strip(), stderr.read().strip()
            if self.remote_command_return[0] == 0:
                return self.remote_command_return[0], self.remote_command_return[1]
            elif self.remote_command_return[0] == 3:
                return "Service not running."
            elif self.remote_command_return[0] == 127:
                return self.remote_command_return[0], remote_command + " not found or not installed!"
            else:
                return self.remote_command_return[0], " ".join([remote_command, self.remote_command_return[1]])
        except Exception, e:
            print formatter.print_red("Couldn't execute command %s on %s !" % (remote_command, host) + e.message)

    def execute_remote_command(self, host, remote_command):
        try:
            self.ssh.connect(host.strip(), username=user.SSH_user_name, password=user.SSH_password, timeout=5,
                             port=self.ssh_port)
            stdin, stdout, stderr = self.ssh.exec_command(remote_command)
            return stdout.channel.recv_exit_status(), "Success - OK"
        except Exception, e:
            print formatter.print_red("Couldn't execute command %s on %s !" % (remote_command, host) + e.message)

    def check_if_service_is_running(self, host, service):
        try:
            cmd_output = self.execute_remote_command(host, CMD_CHECK_IF_SERVICE_IS_RUNNING % service)
            if cmd_output[0] == 0:
                return True
            elif cmd_output[0] == 3:
                return False
            else:
                return None
        except Exception, e:
            print formatter.print_red("Couldn't verify service %s on %s !" % (service, host) + e.message)


formatter = OutputFormatter()
user = UserCredentials()
nvmesh = nvmesh_api.Api()
mgmt = ManagementServer()
hosts = Hosts()


def get_api_ready():
    nvmesh.api_user_name = user.API_user_name
    nvmesh.api_password = user.API_password
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    nvmesh.api_server = mgmt.server
    nvmesh.login()
    return


def show_cluster(csv_format, json_format):
    get_api_ready()
    cluster_json = json.loads(nvmesh.get_cluster())
    capacity_json = json.loads(nvmesh.get_space_allocation())
    total_server = cluster_json['servers']['totalServers']
    offline_server = cluster_json['servers']['offlineServers']
    total_clients = cluster_json['clients']['totalClients']
    offline_clients = cluster_json['clients']['offlineClients']
    cluster_volumes = []
    cluster_list = []
    for volume, count in cluster_json['volumes'].items():
        cluster_volumes.append(' '.join([repr(count), volume]))
    cluster_list.append([total_server, offline_server, total_clients, offline_clients,
                         '; '.join(cluster_volumes),
                         humanfriendly.format_size(capacity_json['totalCapacityInBytes'], binary=True),
                         humanfriendly.format_size(capacity_json['availableSpaceInBytes'], binary=True)])
    if csv_format is True:
        return formatter.print_tsv(cluster_list)
    elif json_format is True:
        return formatter.print_json(cluster_list)
    else:
        return format_smart_table(cluster_list, ['Total Servers', 'Offline Servers', 'Total Clients', 'Offline Clients',
                                                 'Volumes', 'Total Capacity', 'Available Space'])


def show_targets(details, csv_format, json_format, server, short):
    get_api_ready()
    target_json = json.loads(nvmesh.get_servers())
    target_list = []
    for target in target_json:
        if server is not None and target['node_id'].split('.')[0] not in server:
            continue
        else:
            if short is True:
                target_name = target['node_id'].split('.')[0]
            else:
                target_name = target['node_id']
            target_disk_list = []
            target_nic_list = []
            for disk in target['disks']:
                target_disk_list.append(disk['diskID'])
            for nic in target['nics']:
                target_nic_list.append(nic['nicID'])
            if details is True:
                target_list.append([target_name, target['health'], target['version'],
                                    '; '.join(target_disk_list),
                                    ' '.join(target_nic_list)])
            else:
                target_list.append([target_name, target['health'], target['version']])
    if details is True:
        if csv_format is True:
            return formatter.print_tsv(target_list)
        elif json_format is True:
            formatter.print_json(target_list)
            return
        else:
            return format_smart_table(target_list, ['Target Name', 'Target Health', 'NVMesh Version', 'Target Disks',
                                                    'Target NICs'])
    else:
        if csv_format is True:
            return formatter.print_tsv(target_list)
        elif json_format is True:
            return formatter.print_json(target_list)
        else:
            return format_smart_table(target_list, ['Target Name', 'Target Health', 'NVMesh Version'])


def get_target_list():
    get_api_ready()
    target_json = json.loads(nvmesh.get_servers())
    target_list = []
    for target in target_json:
        target_list.append(target['node_id'].split('.')[0])
    return target_list


def get_client_list():
    get_api_ready()
    clients_json = json.loads(nvmesh.get_clients())
    client_list = []
    for client in clients_json:
        client_list.append(client['client_id'].split('.')[0])
    return client_list


def show_clients(csv_format, json_format, server, short):
    get_api_ready()
    clients_json = json.loads(nvmesh.get_clients())
    client_list = []
    for client in clients_json:
        if server is not None and client['client_id'].split('.')[0] not in server:
            continue
        else:
            volume_list = []
            if short is True:
                client_name = client['client_id'].split('.')[0]
            else:
                client_name = client['client_id']
            for volume in client['block_devices']:
                volume_list.append(volume['name'])
            client_list.append([client_name, client['health'], client['version'], '; '.join(volume_list)])
    if csv_format is True:
        return formatter.print_tsv(client_list)
    elif json_format is True:
        return formatter.print_json(client_list)
    else:
        return format_smart_table(client_list, ['Client Name', 'Client Health', 'Client Version', 'Client Volumes'])


def show_volumes(details, csv_format, json_format, volumes, short):
    get_api_ready()
    volumes_json = json.loads(nvmesh.get_volumes())
    volumes_list = []
    remaining_dirty_bits = 0
    for volume in volumes_json:
        if volumes is not None and volume['name'] not in volumes:
            continue
        target_list = []
        target_disk_list = []
        for chunk in volume['chunks']:
            for praid in chunk['pRaids']:
                for segment in praid['diskSegments']:
                    if segment['type'] == 'raftonly':
                        continue
                    else:
                        remaining_dirty_bits = remaining_dirty_bits + segment['remainingDirtyBits']
                        if short is True:
                            target_list.append(segment['node_id'].split('.')[0])
                        else:
                            target_list.append(segment['node_id'])
                        target_disk_list.append(segment['diskID'])
        if 'stripeWidth' in volume:
            stripe_width = volume['stripeWidth']
        else:
            stripe_width = '-'
        if details is True:
            volumes_list.append([volume['name'], volume['health'], volume['status'], volume['RAIDLevel'],
                                 humanfriendly.format_size(volume['capacity'], binary=True), stripe_width,
                                 humanfriendly.format_size((remaining_dirty_bits * 4096), binary=True),
                                 '; '.join(set(target_list)), '; '.join(set(target_disk_list))])
        else:
            volumes_list.append([volume['name'], volume['health'], volume['status'], volume['RAIDLevel'],
                                 humanfriendly.format_size(volume['capacity'], binary=True), stripe_width,
                                 humanfriendly.format_size((remaining_dirty_bits * 4096), binary=True)])
    if details is True:
        if csv_format is True:
            return formatter.print_tsv(volumes_list)
        elif json_format is True:
            return formatter.print_json(volumes_list)
        else:
            return format_smart_table(volumes_list, ['Volume Name', 'Volume Health', 'Volume Status', 'Volume Type',
                                                     'Volume Size', 'Stripe Width', 'Dirty Bits', 'Target Names',
                                                     'Target Disks'])
    else:
        if csv_format is True:
            return formatter.print_tsv(volumes_list)
        elif json_format is True:
            return formatter.print_json(volumes_list)
        else:
            return format_smart_table(volumes_list, ['Volume Name', 'Volume Health', 'Volume Status', 'Volume Type',
                                                     'Volume Size', 'Stripe Width', 'Dirty Bits'])


def show_vpgs(csv_format, json_format, vpgs):
    get_api_ready()
    vpgs_json = json.loads(nvmesh.get_vpgs())
    vpgs_list = []
    for vpg in vpgs_json:
        server_classes_list = []
        disk_classes_list = []
        if vpgs is not None and vpg['name'] not in vpgs:
            continue
        else:
            if 'description' not in vpg:
                vpg_description = ''
            else:
                vpg_description = vpg['description']
            if 'stripeWidth' not in vpg:
                vpg_stripe_width = ''
            else:
                vpg_stripe_width = vpg['stripeWidth']
            for disk_class in vpg['diskClasses']:
                disk_classes_list.append(disk_class)
            for server_class in vpg['serverClasses']:
                server_classes_list.append(server_class)

        vpgs_list.append(
            [vpg['name'], vpg['RAIDLevel'], vpg_stripe_width, humanfriendly.format_size(vpg['capacity'], binary=True),
             '; '.join(disk_classes_list), '; '.join(server_classes_list)])
    if csv_format is True:
        return formatter.print_tsv(vpgs_list)
    elif json_format is True:
        return formatter.print_json(vpgs_list)
    else:
        return format_smart_table(vpgs_list, ['VPG Name', 'RAID Level', 'Stripe Width', 'Reserved Capacity',
                                              'Disk Classes', 'Target Classes'])


def show_drive_classes(details, csv_format, json_format, classes):
    get_api_ready()
    drive_classes_json = json.loads(nvmesh.get_disk_classes())
    drive_class_list = []
    for drive_class in drive_classes_json:
        drive_model_list = []
        drive_target_list = []
        if classes is not None and drive_class['_id'] not in classes:
            continue
        else:
            for disk in drive_class['disks']:
                drive_model_list.append(disk['model'])
                for drive in disk['disks']:
                    if details is True:
                        drive_target_list.append(' '.join([drive['diskID'], drive['node_id']]))
                    else:
                        drive_target_list.append(drive['diskID'])
            drive_class_list.append([drive_class['_id'], '; '.join(drive_model_list), '; '.join(drive_target_list)])
    if csv_format is True:
        return formatter.print_tsv(drive_class_list)
    elif json_format is True:
        return formatter.print_json(drive_class_list)
    else:
        return format_smart_table(drive_class_list, ['Drive Class', 'Drive Models', 'Drive Details'])


def show_target_classes(csv_format, json_format, classes):
    get_api_ready()
    target_classes_json = json.loads(nvmesh.get_target_classes())
    target_classes_list = []
    for target_class in target_classes_json:
        if classes is not None and target_class['_id'] not in classes:
            continue
        else:
            target_nodes = []
            if 'description' not in target_class:
                target_class_description = ''
            else:
                target_class_description = target_class['description']
            for node in target_class['targetNodes']:
                target_nodes.append(node)
        target_classes_list.append([target_class['name'], target_class_description, '; '.join(target_nodes)])
    if csv_format is True:
        return formatter.print_tsv(target_classes_list)
    elif json_format is True:
        return formatter.print_json(target_classes_list)
    else:
        return format_smart_table(target_classes_list, ['Target Class', 'Description', 'Target Nodes'])


def count_active_targets():
    active_targets = 0
    for target in get_target_list():
        ssh = SSHRemoteOperations()
        ssh_return = ssh.return_remote_command_std_output(target, CMD_STATUS_NVMESH_TARGET)
        if ssh_return[0] == 0:
            active_targets += 1
    return active_targets


def manage_nvmesh_service(scope, details, servers, action, prefix, parallel):
    output = []
    ssh = SSHRemoteOperations()
    host_list = []
    ssh_return = []
    if servers is not None:
        host_list = set(servers)
    else:
        if scope == 'cluster':
            host_list = get_target_list()
            host_list.extend(get_client_list())
            host_list.extend(mgmt.get_management_server_list())
        if scope == 'target':
            host_list = get_target_list()
        if scope == 'client':
            host_list = get_client_list()
        if scope == 'mgr':
            host_list = mgmt.get_management_server_list()
    if parallel is True:
        process_pool = Pool(len(set(host_list)))
        parallel_execution_map = []
        for host in set(host_list):
            if action == "check":
                parallel_execution_map.append([host, "service nvmesh%s status" % scope])
            elif action == "start":
                parallel_execution_map.append([host, "service nvmesh%s start" % scope])
            elif action == "stop":
                parallel_execution_map.append([host, "service nvmesh%s stop" % scope])
            elif action == "restart":
                parallel_execution_map.append([host, "service nvmesh%s restart" % scope])
        command_return_list = process_pool.map(run_parallel_ssh_command, parallel_execution_map)
        process_pool.close()
        for command_return in command_return_list:
            if command_return[1][0] == 0:
                if details is True:
                    output.append(formatter.bold(" ".join([command_return[0], action.capitalize(),
                                                 formatter.green('OK')])))
                    if prefix is True:
                        output.append(formatter.add_line_prefix(command_return[0], (
                            command_return[1][1][:command_return[1][1].rfind('\n')])) + "\n")
                    else:
                        output.append((command_return[1][1][:command_return[1][1].rfind('\n')] + "\n"))
                else:
                    output.append(" ".join([command_return[0], action.capitalize(), formatter.green('OK')]))
            else:
                if details is True:
                    output.append(formatter.bold(" ".join([command_return[0], action.capitalize(),
                                                           formatter.red('Failed')])))
                    if prefix is True:
                        output.append(formatter.add_line_prefix(command_return[0], (
                            command_return[1][1]) + "\n"))
                    else:
                        output.append(command_return[1][1] + "\n")
                else:
                    output.append(" ".join([command_return[0], action.capitalize(), formatter.red('Failed')]))
        return "\n".join(output)
    else:
        for server in host_list:
            if action == "check":
                ssh_return = ssh.return_remote_command_std_output(server, "service nvmesh%s status" % scope)
            elif action == "start":
                ssh_return = ssh.return_remote_command_std_output(server, "service nvmesh%s start" % scope)
            elif action == "stop":
                ssh_return = ssh.return_remote_command_std_output(server, "service nvmesh%s stop" % scope)
            elif action == "restart":
                ssh_return = ssh.return_remote_command_std_output(server, "service nvmesh%s restart" % scope)
            if ssh_return[0] == 0:
                if details is True:
                    output.append(' '.join([formatter.bold(server), action.capitalize(), formatter.green('OK')]))
                    if prefix is True:
                        output.append(formatter.add_line_prefix(server, (ssh_return[1])))
                    else:
                        output.append((ssh_return[1] + "\n"))
                else:
                    output.append(" ".join([server, action.capitalize(), formatter.green('OK')]))
            else:
                if details is True:
                    output.append(' '.join([formatter.bold(server), action.capitalize(), formatter.red('Failed')]))
                    if prefix is True:
                        output.append(formatter.add_line_prefix(server, (ssh_return[1])))
                    else:
                        output.append((ssh_return[1] + "\n"))
                else:
                    output.append(" ".join([server, action.capitalize(), formatter.red('Failed')]))
        return "\n".join(output)


def manage_mcm(clients, action):
    for client in get_client_list():
        if clients is not None and client.split('.')[0] not in clients:
            continue
        else:
            ssh = SSHRemoteOperations()
            if action == "stop":
                ssh.execute_remote_command(client, CMD_STOP_NVMESH_MCM)
                print client, "\tStopped the MangaementCM services."
            elif action == "start":
                ssh.execute_remote_command(client, CMD_START_NVMESH_MCM)
                print client, "\tStarted the MangaementCM services."
            elif action == "restart":
                ssh.execute_remote_command(client, CMD_RESTART_NVMESH_MCM)
                print client, "\tRestarted the MangaementCM services."


def manage_cluster(details, action, prefix):
    if action == "check":
        print("Checking the NVMesh managers ...")
        NvmeshShell().poutput(manage_nvmesh_service('mgr', details, None, action, prefix, True))
        print("Checking the NVMesh targets ...")
        NvmeshShell().poutput(manage_nvmesh_service('target', details, None, action, prefix, True))
        print("Checking the NVMesh clients ...")
        NvmeshShell().poutput(manage_nvmesh_service('client', details, None, action, prefix, True))
    elif action == "start":
        print ("Starting the NVMesh managers ...")
        NvmeshShell().poutput(manage_nvmesh_service('mgr', details, None, action, prefix, True))
        time.sleep(3)
        print ("Starting the NVMesh targets ...")
        NvmeshShell().poutput(manage_nvmesh_service('target', details, None, action, prefix, True))
        print ("Starting the NVMesh clients ...")
        NvmeshShell().poutput(manage_nvmesh_service('client', details, None, action, prefix, True))
    elif action == "stop":
        print ("Stopping the NVMesh clients ...")
        NvmeshShell().poutput(manage_nvmesh_service('client', details, None, action, prefix, True))
        print ("Stopping the NVMesh targets ...")
        NvmeshShell().poutput(manage_nvmesh_service('target', details, None, action, prefix, True))
        print ("Stopping the NVMesh managers ...")
        NvmeshShell().poutput(manage_nvmesh_service('mgr', details, None, action, prefix, True))
    elif action == "restart":
        print ("Stopping the NVMesh clients ...")
        NvmeshShell().poutput(manage_nvmesh_service('client', details, None, 'stop', prefix, True))
        print ("Stopping the NVMesh targets ...")
        NvmeshShell().poutput(manage_nvmesh_service('target', details, None, 'stop', prefix, True))
        print ("Restarting the NVMesh managers ...")
        NvmeshShell().poutput(manage_nvmesh_service('mgr', details, None, 'restart', prefix, True))
        time.sleep(3)
        print ("Starting the NVMesh targets ...")
        NvmeshShell().poutput(manage_nvmesh_service('target', details, None, 'start', prefix, True))
        print ("Starting the NVMesh clients ...")
        NvmeshShell().poutput(manage_nvmesh_service('client', details, None, 'start', prefix, True))


def run_command(command, scope, prefix, parallel, server_list):
    command_output = []
    host_list = []
    ssh = SSHRemoteOperations()
    command_line = " ".join(command)
    if server_list is not None:
        host_list = server_list
    else:
        if scope == 'cluster':
            host_list = get_target_list()
            host_list.extend(get_client_list())
            host_list.extend(mgmt.get_management_server_list())
        if scope == 'targets':
            host_list = get_target_list()
        if scope == 'clients':
            host_list = get_client_list()
        if scope == 'managers':
            host_list = mgmt.get_management_server_list()
    if parallel is True:
        process_pool = Pool(len(set(host_list)))
        parallel_execution_map = []
        for host in set(host_list):
            parallel_execution_map.append([host, command_line])
        command_return_list = process_pool.map(run_parallel_ssh_command, parallel_execution_map)
        process_pool.close()
        output = []
        if prefix is True:
            for command_return in command_return_list:
                output.append(formatter.add_line_prefix(command_return[0], command_return[1]))
            return "\n".join(output)
        else:
            for command_return in command_return_list:
                output.append(command_return[1])
            return "\n".join(output)
    else:
        for host in set(host_list):
            if prefix is True:
                command_output.append(formatter.add_line_prefix(host, ssh.return_remote_command_std_output(
                    host, command_line)[1]))
            else:
                command_output.append(ssh.return_remote_command_std_output(host, command_line)[1])
        return "\n".join(command_output)


def run_parallel_ssh_command(argument):
    ssh = SSHRemoteOperations()
    output = ssh.return_remote_command_std_output(argument[0], argument[1])
    return argument[0], output


class NvmeshShell(Cmd):

    def __init__(self):
        Cmd.__init__(self, use_ipython=True)

    prompt = "\033[1;34mnvmesh #\033[0m "
    show_parser = argparse.ArgumentParser()
    show_parser.add_argument('nvmesh_object', choices=['cluster', 'targets', 'clients', 'volumes', 'manager',
                                                       'sshuser', 'apiuser', 'vpgs', 'driveclasses', 'targetclasses',
                                                       'hosts'],
                             default='cluster',
                             nargs='?',
                             help='Define/specify the NVMesh object you want to list or view.')
    show_parser.add_argument('-a', '--all', required=False, default=True, action='store_const', const=True,
                             help='List and view all NVMesh objects throughout the cluster')
    show_parser.add_argument('-c', '--classes', nargs='+', required=False,
                             help='View a single or a list of NVMesh drive or target classes.')
    show_parser.add_argument('-d', '--details', required=False, action='store_const', const=True,
                             help='Show more details.')
    show_parser.add_argument('-j', '--json', required=False, action='store_const', const=True,
                             help='Format output as JSON.')
    show_parser.add_argument('-s', '--servers', nargs='+', required=False,
                             help='List or view NVMesh objects of a single server or a list of servers.')
    show_parser.add_argument('-S', '--short-names', required=False, action='store_const', const=True,
                             help='Show short hostnames.')
    show_parser.add_argument('-t', '--tsv', required=False, action='store_const', const=True,
                             help='Format output as tabulator separated values.')
    show_parser.add_argument('-v', '--volumes', nargs='+', required=False,
                             help='View a single NVMesh volume or a list of volumes.')
    show_parser.add_argument('-p', '--vpgs', nargs='+', required=False,
                             help='View a single or a list of NVMesh volume provisioning groups.')

    @with_argparser(show_parser)
    def do_show(self, args):
        """List and view specific Nvmesh objects and its properties.
The 'list sub-command allows output in a table, tabulator separated value or JSON format.
By default it will list all the objects and their properties in the cluster.
E.g 'list targets' will list all targets.
In case you want to see the properties of only one or just a few you need to use the '-s' or '--server'
option to specify single or a list of servers/targets.
E.g. 'list targets -s target1 target2'"""
        if args.nvmesh_object == 'targets':
            self.poutput(show_targets(args.details, args.tsv, args.json, args.servers, args.short_names))
        elif args.nvmesh_object == 'clients':
            self.poutput(show_clients(args.tsv, args.json, args.servers, args.short_names))
        elif args.nvmesh_object == 'volumes':
            self.poutput(show_volumes(args.details, args.tsv, args.json, args.volumes, args.short_names))
        elif args.nvmesh_object == 'sshuser':
            print(user.SSH_user_name)
        elif args.nvmesh_object == 'apiuser':
            print(user.API_user_name)
        elif args.nvmesh_object == 'manager':
            print(mgmt.server)
        elif args.nvmesh_object == 'cluster':
            self.poutput(show_cluster(args.tsv, args.json))
        elif args.nvmesh_object == 'vpgs':
            self.poutput(show_vpgs(args.tsv, args.json, args.vpgs))
        elif args.nvmesh_object == 'driveclasses':
            self.poutput(show_drive_classes(args.details, args.tsv, args.json, args.classes))
        elif args.nvmesh_object == 'targetclasses':
            self.poutput(show_target_classes(args.tsv, args.json, args.classes))
        elif args.nvmesh_object == 'hosts':
            self.poutput(hosts.manage_hosts("list", None))

    add_parser = argparse.ArgumentParser()
    add_parser.add_argument('nvmesh_object', choices=['host', 'volume'], nargs=1,
                            help='Add hosts to this shell environment')
    add_parser.add_argument('-r', '--raid_level', nargs=1, required=False,
                            help='The RAID level of the volume. Options: LVM_JBOD, RAID0, RAID1, RAID10')
    add_parser.add_argument('-v', '--vpg', nargs=1, required=False,
                            help='Optional - The volume provisioning group to use.')
    add_parser.add_argument('-o', '--domain', nargs=1, required=False,
                            help='Optional - Domain to use.')
    add_parser.add_argument('-D', '--description', nargs=1, required=False,
                            help='Optional - Description')
    add_parser.add_argument('-l', '--limit-by-disk', nargs='+', required=False,
                            help='Optional - Limit volume allocation to specific drives.')
    add_parser.add_argument('-L', '--limit-by-targets', nargs='+', required=False,
                            help='Optional - Limit volume allocation to specific target nodes.')
    add_parser.add_argument('-n', '--name', nargs=1, required=False,
                            help='Name of the volume, must be unique, will be the ID of the volume.')
    add_parser.add_argument('-c', '--count', nargs=1, required=False, default=1,
                            help='Number of volumes to create and add.')
    add_parser.add_argument('-t', '--target-class', nargs='+', required=False,
                            help='Optional - Limit volume allocation to specific target classes.')
    add_parser.add_argument('-d', '--drive-class', nargs='+', required=False,
                            help='Optional - Limit volume allocation to specific drive classes.')
    add_parser.add_argument('-w', '--stripe-width', nargs=1, required=False,
                            help='Number of disks to use. Required for R0 and R1.')
    add_parser.add_argument('-s', '--server', nargs='+', required=False,
                            help='Specify a single server or a list of servers.')
    add_parser.add_argument('-S', '--size', nargs='+', required=False,
                            help='Specify a the size of the new volume. The volumes size value is base*2/binary. '
                                 'Example: -S 12GB or 12GiB will create a volume with a size of 12884901888 bytes.'
                                 'Some valid input formats samples: xGB, x GB, x gigabyte, x GiB or xG')

    @with_argparser(add_parser)
    def do_add(self, args):
        """The 'add' sub-comand will let you add nvmesh objects to your cluster or nvmesh-shell runtime environment.
E.g. 'add hosts' will add host/server entries to your nvmesh-shell environment while 'add volume' will create and add
a new volume to the NVMesh cluster.
"""
        action = "add"
        if args.nvmesh_object == 'host':
            hosts.manage_hosts(action, args.server)

    delete_parser = argparse.ArgumentParser()
    delete_parser.add_argument('nvmesh_object', choices=['hosts'],
                               nargs="?",
                               help='Add hosts/servers to this shell environment')
    delete_parser.add_argument('-s', '--server', nargs='+', required=False,
                               help='Specify a single server or a list of servers.')

    @with_argparser(delete_parser)
    def do_delete(self, args):
        """The 'delete' sub-comand will let you delete nvmesh objects in your cluster or nvmesh-shell
runtime environment.
E.g. 'delete hosts' will delete host/server entries in your nvmesh-shell environment.
"""
        action = "delete"
        if args.nvmesh_object == 'hosts':
            hosts.manage_hosts(action, args.server)

    check_parser = argparse.ArgumentParser()
    check_parser.add_argument('nvmesh_object', choices=['clients', 'targets', 'managers', 'cluster'],
                              nargs='?', default='cluster',
                              help='Specify where you want to check the NVMesh services status.')
    check_parser.add_argument('-d', '--details', required=False, action='store_const', const=True,
                              help='List and view the detailed service information.')
    check_parser.add_argument('-p', '--host-prefix', required=False, action='store_const', const=True,
                              help='Adds the host name at the beginning of each line. This helps to identify the '
                                   'content when piping into a grep or similar')
    check_parser.add_argument('-P', '--parallel', required=False, action='store_const', const=True,
                               help='Check the hosts/servers in parallel.')
    check_parser.add_argument('-s', '--servers', nargs='+', required=False,
                              help='Specify a single or a list of managers, targets or clients.')

    @with_argparser(check_parser)
    def do_check(self, args):
        """The 'check' sub-command checks and let you list the status of the actual NVMesh services running in your
cluster. It is using SSH connectivity to the NVMesh managers, clients and targets to verify the service status.
E.g. 'check targets' will check the NVMesh target services throughout the cluster."""
        action = "check"
        if args.nvmesh_object == 'targets':
            self.poutput(manage_nvmesh_service('target', args.details, args.servers, action, args.host_prefix,
                                               args.parallel))
        elif args.nvmesh_object == 'clients':
            self.poutput(manage_nvmesh_service('client', args.details, args.servers, action, args.host_prefix,
                                               args.parallel))
        elif args.nvmesh_object == 'managers':
            self.poutput(manage_nvmesh_service('mgr', args.details, args.servers, action, args.host_prefix,
                                               args.parallel))
        elif args.nvmesh_object == 'cluster':
            manage_cluster(args.details, action, args.host_prefix)

    stop_parser = argparse.ArgumentParser()
    stop_parser.add_argument('nvmesh_object', choices=['clients', 'targets', 'managers', 'cluster', 'mcm'],
                             nargs='?', default='cluster',
                             help='Specify the NVMesh service type you want to stop.')
    stop_parser.add_argument('-d', '--details', required=False, action='store_const', const=True,
                             help='List and view the service details.')
    stop_parser.add_argument('-g', '--graceful', nargs=1, required=False, default=True, choices=['True', 'False'],
                             help="Graceful stop of all NVMesh targets in the cluster."
                                  " The default is set to 'True'")
    stop_parser.add_argument('-p', '--host-prefix', required=False, action='store_const', const=True,
                             help='Adds the host name at the beginning of each line. This helps to identify the '
                                  'content when piping into a grep or similar')
    stop_parser.add_argument('-P', '--parallel', required=False, action='store_const', const=True,
                              help='Stop the NVMesh services on the hosts/servers in parallel.')
    stop_parser.add_argument('-s', '--servers', nargs='+', required=False,
                             help='Specify a single or a list of managers, targets or clients.')

    @with_argparser(stop_parser)
    def do_stop(self, args):
        """The 'stop' sub-command will stop the selected NVMesh services on all managers, targets and clients.
Or it will stop the entire NVMesh cluster. It uses SSH connectivity to manage the NVMesh services.
E.g. 'stop clients' will stop all the NVMesh clients throughout the cluster."""
        action = "stop"
        if args.nvmesh_object == 'targets':
            self.poutput(manage_nvmesh_service('target', args.details, args.servers, action, args.host_prefix,
                                               args.parallel))
        elif args.nvmesh_object == 'clients':
            self.poutput(manage_nvmesh_service('client', args.details, args.servers, action, args.host_prefix,
                                               args.parallel))
        elif args.nvmesh_object == 'managers':
            self.poutput(manage_nvmesh_service('mgr', args.details, args.servers, action, args.host_prefix,
                                               args.parallel))
        elif args.nvmesh_object == 'cluster':
            manage_cluster(args.details, action, args.host_prefix)
        elif args.nvmesh_object == 'mcm':
            manage_mcm(args.server, action)

    start_parser = argparse.ArgumentParser()
    start_parser.add_argument('nvmesh_object', choices=['clients', 'targets', 'managers', 'cluster', 'mcm'],
                              nargs='?', default='cluster',
                              help='Specify the NVMesh service type you want to stop')
    start_parser.add_argument('-d', '--details', required=False, action='store_const', const=True,
                              help='List and view the service details.')
    start_parser.add_argument('-p', '--host-prefix', required=False, action='store_const', const=True,
                              help='Adds the host name at the beginning of each line. This helps to identify the '
                                   'content when piping into a grep or similar')
    start_parser.add_argument('-P', '--parallel', required=False, action='store_const', const=True,
                              help='Start the NVMesh services on the hosts/servers in parallel.')
    start_parser.add_argument('-s', '--servers', nargs='+', required=False,
                              help='Specify a single or a list of servers.')

    @with_argparser(start_parser)
    def do_start(self, args):
        """The 'start' sub-command will start the selected NVMesh services on all managers, targets and clients.
Or it will start the entire NVMesh cluster. It uses SSH connectivity to manage the NVMesh services.
E.g. 'start cluster' will start all the NVMesh services throughout the cluster."""
        action = "start"
        if args.nvmesh_object == 'targets':
            self.poutput(manage_nvmesh_service('target', args.details, args.servers, action, args.host_prefix,
                                               args.parallel))
        elif args.nvmesh_object == 'clients':
            self.poutput(manage_nvmesh_service('client', args.details, args.servers, action, args.host_prefix,
                                               args.parallel))
        elif args.nvmesh_object == 'managers':
            self.poutput(manage_nvmesh_service('mgr', args.details, args.servers, action, args.host_prefix,
                                               args.parallel))
        elif args.nvmesh_object == 'cluster':
            manage_cluster(args.details, action, args.host_prefix)
        elif args.nvmesh_object == 'mcm':
            manage_mcm(args.server, action)

    restart_parser = argparse.ArgumentParser()
    restart_parser.add_argument('nvmesh_object', choices=['clients', 'targets', 'managers', 'cluster', 'mcm'],
                                nargs='?', default='cluster',
                                help='Specify the NVMesh service which you want to restart.')
    restart_parser.add_argument('-d', '--details', required=False, action='store_const', const=True,
                                help='List and view the service details.')
    restart_parser.add_argument('-g', '--graceful', nargs=1, required=False, default=True, choices=['True', 'False'],
                                help='Restart with a graceful stop of the targets in the cluster.'
                                     'The default is set to True')
    restart_parser.add_argument('-p', '--host-prefix', required=False, action='store_const', const=True,
                                help='Adds the host name at the beginning of each line. This helps to identify the '
                                     'content when piping into a grep or similar')
    restart_parser.add_argument('-P', '--parallel', required=False, action='store_const', const=True,
                              help='Restart the NVMesh services on the hosts/servers in parallel.')
    restart_parser.add_argument('-s', '--servers', nargs='+', required=False,
                                help='Specify a single or a list of servers.')

    @with_argparser(restart_parser)
    def do_restart(self, args):
        """The 'restart' sub-command will restart the selected NVMesh services on all managers, targets and clients.
Or it will restart the entire NVMesh cluster. It uses SSH connectivity to manage the NVMesh services.
E.g. 'restart managers' will restart the NVMesh management service."""
        action = 'restart'
        if args.nvmesh_object == 'targets':
            self.poutput(manage_nvmesh_service('target', args.details, args.servers, action, args.host_prefix,
                                               args.parallel))
        elif args.nvmesh_object == 'clients':
            self.poutput(manage_nvmesh_service('client', args.details, args.servers, action, args.host_prefix,
                                               args.parallel))
        elif args.nvmesh_object == 'managers':
            self.poutput(manage_nvmesh_service('mgr', args.details, args.servers, action, args.host_prefix,
                                               args.parallel))
        elif args.nvmesh_object == 'mcm':
            manage_mcm(args.server, action)
        elif args.nvmesh_object == 'cluster':
            manage_cluster(args.details, action, args.host_prefix)

    define_parser = argparse.ArgumentParser()
    define_parser.add_argument('nvmesh_object', choices=['manager', 'sshuser', 'sshpassword', 'apiuser', 'apipassword'],
                               nargs='?',
                               help='Specify the NVMesh shell runtime variable you want to define.')
    define_parser.add_argument('-t', '--persistent', required=False, action='store_const', const=True,
                               help='Define/Set the NVMesh runtime variable persistently.')
    define_parser.add_argument('-p', '--password', nargs=1, required=False,
                               help='The password for the user to be used.')
    define_parser.add_argument('-u', '--user', nargs=1, required=False,
                               help='The username name for the user to be used.')
    define_parser.add_argument('-s', '--server', nargs='+', required=False,
                               help='Define/Set the NVMesh management server')

    @with_argparser(define_parser)
    def do_define(self, args):
        """The 'define' sub-command defines/sets the shell runtime variables. It can be used to set them temporarily or
persistently. Please note that in its current version allows to set only one NVMesh manager. If you try to provide a
list it will use the first manager name out of that list.
E.g. 'define apiuser' will set the NVMesh API user name to be used for all the operations involving the API"""
        if args.nvmesh_object == 'sshuser':
            if args.user is None:
                user.SSH_user_name = raw_input("Please provide the root level SSH user name: ")
            else:
                user.SSH_user_name = args.user[0]
            if args.persistent is True:
                user.save_ssh_user()
        elif args.nvmesh_object == 'apiuser':
            if args.user is None:
                user.API_user_name = raw_input("Please provide the root level API user name: ")
            else:
                user.API_user_name = args.user[0]
            if args.persistent is True:
                user.save_api_user()
        elif args.nvmesh_object == 'sshpassword':
            user.SSH_password = getpass.getpass("Please provide the SSH password: ")
            if args.persistent is True:
                user.save_ssh_user()
        elif args.nvmesh_object == 'apipassword':
            user.API_password = getpass.getpass("Please provide the API password: ")
            if args.persistent is True:
                user.save_api_user()
        elif args.nvmesh_object == 'manager':
            if args.server is None:
                nvmesh.api_server = raw_input("Please provide the NVMesh management server name: ")
            else:
                mgmt.server = args.server[0]
            if args.persistent is True:
                mgmt.save_management_server(args.server)
                mgmt.server = args.server[0]

    def do_license(self, args):
        """Shows the licensing details, term and conditions. """
        self.ppaged(open('LICENSE.txt', 'r').read())

    runcmd_parser = argparse.ArgumentParser()
    runcmd_parser.add_argument('scope', choices=['clients', 'targets', 'managers', 'cluster'],
                               nargs='?', default='cluster',
                               help='Specify the scope where you want to run the command.')
    runcmd_parser.add_argument('-c', '--command', nargs='+', required=True,
                               help='The command you want to run on the servers.')
    runcmd_parser.add_argument('-p', '--host-prefix', required=False, action='store_const', const=True,
                               help='Adds the host name at the beginning of each line. This helps to identify the '
                                    'content when piping into a grep or similar tasks.')
    runcmd_parser.add_argument('-P', '--parallel', required=False, action='store_const', const=True,
                               help='Runs the remote command on the remote hosts in parallel.')
    runcmd_parser.add_argument('-s', '--servers', nargs='+', required=False,
                               help='Specify list of servers and or hosts.')

    @with_argparser(runcmd_parser)
    def do_runcmd(self, args):
        """Run a remote shell command across the whole NVMesh cluster, or just the targets, clients, managers or a list
        of selected servers and hosts.
Excample: runcmd managers -c systemctl status mongod"""
        self.poutput(run_command(args.command, args.scope, args.host_prefix, args.parallel, args.servers))


def start_shell():
    reload(sys)
    sys.setdefaultencoding('utf-8')
    history_file = os.path.expanduser('~/.nvmesh_shell_history')
    if not os.path.exists(history_file):
        with open(history_file, "w") as history:
            history.write("")
    readline.read_history_file(history_file)
    atexit.register(readline.write_history_file, history_file)
    mgmt.server = mgmt.get_management_server()
    user.get_api_user()
    user.get_ssh_user()
    shell = NvmeshShell()
    if len(sys.argv) > 1:
        shell.onecmd(' '.join(sys.argv[1:]))
    else:
        shell.cmdloop('''
Copyright (c) 2018 Excelero, Inc. All rights reserved.

This program comes with ABSOLUTELY NO WARRANTY; for licencing and warranty details type 'license'.
This is free software, and you are welcome to redistribute it under certain conditions; type 'license' for details.

Starting the NVMesh shell ...''')


if __name__ == '__main__':
    start_shell()
