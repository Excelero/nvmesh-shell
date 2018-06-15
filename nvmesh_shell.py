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


class ArgsUsageOutputFormatter(argparse.HelpFormatter):
    # use defined argument order to display usage
    def _format_usage(self, usage, actions, groups, prefix):
        if prefix is None:
            prefix = 'usage: '

        # if usage is specified, use that
        if usage is not None:
            usage = usage % dict(prog=self._prog)

        # if no optionals or positionals are available, usage is just prog
        elif usage is None and not actions:
            usage = '%(prog)s' % dict(prog=self._prog)
        elif usage is None:
            prog = '%(prog)s' % dict(prog=self._prog)
            # build full usage string
            action_usage = self._format_actions_usage(actions, groups)  # NEW
            usage = ' '.join([s for s in [prog, action_usage] if s])
            # omit the long line wrapping code
        # prefix with 'usage:'
        return '%s%s\n\n' % (prefix, usage)


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
        return '\n'.join(text_lines)


class Payload(dict):
    def __str__(self):
        return json.dumps(self)

    def __repr__(self):
        return json.dumps(self)


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
        elif action == "get":
            if os.path.isfile(self.host_file):
                output = []
                self.host_list = open(self.host_file, 'r').readlines()
                for host in self.host_list:
                    output.append(host.strip())
                return output
            else:
                return [self.formatter.yellow(
                    "No hosts defined! Use 'add hosts' to add hosts to your shell environment.")]
        elif action == "delete":
            tmp_host_list = []
            if os.path.isfile(self.host_file):
                for line in open(self.host_file, 'r').readlines():
                    tmp_host_list.append(line.strip())
                for host in hosts_list:
                    tmp_host_list.remove(host.strip())
                open(self.host_file, 'w').write(('\n'.join(tmp_host_list) + '\n'))
            else:
                return [self.formatter.yellow(
                    "No hosts defined! Use 'add hosts' to add hosts to your shell environment.")]


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
            for manager in open(self.server_file, 'r').readlines():
                self.server_list.append(manager.split('.')[0])
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
        self.SSH_secrets = None
        self.API_secrets = None

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
            self.SSH_secrets = open(self.SSH_secrets_file, 'r').read().split(' ')
        except Exception, e:
            pass
        if self.SSH_secrets is None:
            formatter.print_yellow("SSH user credentials not set yet!")
            self.SSH_user_name = raw_input("Provide the root level SSH user name: ")
            self.SSH_password = getpass.getpass("Please provide the SSH password: ")
            self.save_ssh_user()
            return self.SSH_user_name
        else:
            self.SSH_user_name = self.SSH_secrets[0]
            self.SSH_password = base64.b64decode(self.SSH_secrets[1])
            return self.SSH_user_name

    def get_api_user(self):
        try:
            self.API_secrets = open(self.API_secrets_file, 'r').read().split(' ')
        except Exception, e:
            pass
        if self.API_secrets is None:
            formatter.print_yellow("API user credentials not set yet!")
            self.API_user_name = raw_input("Provide the root level API user name: ")
            self.API_password = getpass.getpass("Please provide the API password: ")
            self.save_api_user()
            return self.API_user_name
        else:
            self.API_user_name = self.API_secrets[0]
            self.API_password = base64.b64decode(self.API_secrets[1])
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

    def test_ssh_connection(self, host):
        try:
            self.ssh.connect(
                host, username=user.SSH_user_name, password=user.SSH_password, timeout=5, port=self.ssh_port)
            return formatter.green("Connection to host %s OK" % host)
        except Exception, e:
            print formatter.red("Connection to host %s Failed! " % host + e.message)
        self.ssh.close()
        return

    def transfer_files(self, host, list_of_files):
        try:
            self.ssh.connect(
                host, username=user.SSH_user_name, password=user.SSH_password, timeout=5, port=self.ssh_port)
            self.sftp = self.ssh.open_sftp()
            try:
                self.sftp.chdir(self.remote_path)
            except IOError:
                self.sftp.mkdir(self.remote_path)
            for file_to_transfer in list_of_files:
                self.sftp.put(self.local_path + "/" + file_to_transfer, self.remote_path + "/" + file_to_transfer)
            self.sftp.close()
            self.ssh.close()
            return formatter.green("File transfer to host %s OK" % host)
        except Exception, e:
            return formatter.red("File transfer to %s Failed! " % host + e.message)

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
    mgmt.server = mgmt.get_management_server()
    user.get_api_user()
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


def get_client_list(full):
    get_api_ready()
    clients_json = json.loads(nvmesh.get_clients())
    client_list = []
    for client in clients_json:
        if full is True:
            client_list.append(client['client_id'])
        else:
            client_list.append(client['client_id'].split('.')[0])
    return client_list


def get_volume_list():
    get_api_ready()
    volume_json = json.loads(nvmesh.get_volumes())
    volume_list = []
    for volume in volume_json:
        volume_list.append(volume['_id'].split('.')[0])
    return volume_list


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
    for volume in volumes_json:
        remaining_dirty_bits = 0
        if volumes is not None and volume['name'] not in volumes:
            continue
        else:
            if 'stripeWidth' in volume:
                stripe_width = volume['stripeWidth']
            else:
                stripe_width = "-"
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
            host_list.extend(get_client_list(False))
            host_list.extend(mgmt.get_management_server_list())
        if scope == 'target':
            host_list = get_target_list()
        if scope == 'client':
            host_list = get_client_list(False)
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
    if clients is not None:
        client_list = clients
    else:
        client_list = get_client_list()
        ssh = SSHRemoteOperations()
    for client in client_list:
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
    host_list = []
    ssh = SSHRemoteOperations()
    command_line = " ".join(command)
    if server_list is not None:
        host_list = server_list
    else:
        if scope == 'cluster':
            host_list = get_target_list()
            host_list.extend(get_client_list(False))
            host_list.extend(mgmt.get_management_server_list())
        if scope == 'targets':
            host_list = get_target_list()
        if scope == 'clients':
            host_list = get_client_list(False)
        if scope == 'managers':
            host_list = mgmt.get_management_server_list()
        if scope == 'hosts':
            host_list = Hosts().manage_hosts('get', None)
    host_list = set(host_list)
    command_return_list = []
    if parallel is True:
        process_pool = Pool(len(host_list))
        parallel_execution_map = []
        for host in host_list:
            parallel_execution_map.append([host, command_line])
        command_return_list = process_pool.map(run_parallel_ssh_command, parallel_execution_map)
        process_pool.close()
    else:
        for host in host_list:
            command_return_list.append([host, ssh.return_remote_command_std_output(host, command_line)])
    output = []
    for command_return in command_return_list:
        if command_return[1][0] != 0:
            output_line = formatter.red(" ".join(["Return Code %s," % (
                command_return[1][0]), command_return[1][1]]))
            if prefix is True:
                output.append(formatter.add_line_prefix(command_return[0], output_line))
            else:
                output.append(output_line)
        else:
            if len(command_return[1][1]) < 1:
                output_line = formatter.green("OK")
            else:
                output_line = command_return[1][1]
            if prefix is True:
                output.append(formatter.add_line_prefix(command_return[0], output_line))
            else:
                output.append(output_line)
    return "\n".join(output)


def run_parallel_ssh_command(argument):
    ssh = SSHRemoteOperations()
    output = ssh.return_remote_command_std_output(argument[0], argument[1])
    return argument[0], output


def manage_volume(action, name, capacity, description, disk_classes, server_classes, limit_by_nodes, limit_by_disks,
                  awareness, raid_level, stripe_width, number_of_mirrors, vpg):
    get_api_ready()
    api_payload = {}
    payload = {}
    if action == "create":
        if capacity is None:
            return formatter.yellow(
                "Size/capacity information missing! Use the -S argument to provide the volume size/capacity")
        payload = {
            "name": name,
            "capacity": "MAX" if str(capacity[0]).upper() == "MAX" else int(humanfriendly.parse_size(capacity[0],
                                                                                                     binary=True)),
        }
        if description is not None:
            payload["description"] = description[0]
        if disk_classes is not None:
            payload["diskClasses"] = disk_classes
        if server_classes is not None:
            payload["serverClasses"] = server_classes
        if limit_by_nodes is not None:
            payload["limitByNodes"] = limit_by_nodes
        if limit_by_disks is not None:
            payload["limitByDisks"] = limit_by_disks
        if awareness is not None:
            payload["awareness"] = awareness[0]
        if raid_level is None and vpg is None:
            return formatter.yellow(
                "Raid level information missing! Use the -r argument to set the raid level.")
        if raid_level is not None and vpg is None:
            payload["RAIDLevel"] = RAID_LEVELS[raid_level[0]]
            if raid_level[0] == "lvm":
                pass
            elif raid_level[0] == "0":
                payload["stripeSize"] = 32
                if stripe_width is None:
                    return formatter.yellow(
                        "Stripe width information missing! Use the -w argument to set the stripe width.")
                payload["stripeWidth"] = int(stripe_width[0])
            elif raid_level[0] == "1":
                payload["numberOfMirrors"] = int(number_of_mirrors[0]) if number_of_mirrors is not None else 1
            elif raid_level[0] == "10":
                payload["stripeSize"] = 32
                if stripe_width is None:
                    return formatter.yellow(
                        "Stripe width information missing! Use the -w argument to set the stripe width.")
                payload["stripeWidth"] = int(stripe_width[0])
                payload["numberOfMirrors"] = int(number_of_mirrors[0]) if number_of_mirrors is not None else 1
        if vpg is not None:
            payload["VPG"] = vpg[0]
        api_payload["create"] = [payload]
        api_payload["remove"] = []
        api_payload["edit"] = []
        api_return = json.loads(nvmesh.manage_volume(api_payload))
        if api_return['create'][0]['success'] is True:
            return " ".join(["Volume", name, "successfully created.", formatter.green('OK')])

        else:
            return " ".join([api_return['create'][0]['err'], formatter.red('Failed')])

    elif action == 'remove':
        api_return = []
        output = []
        for volume in name:
            payload["_id"] = volume
            api_payload["remove"] = [payload]
            api_payload["create"] = []
            api_payload["edit"] = []
            api_return.append(json.loads(nvmesh.manage_volume(api_payload)))
        for item in api_return:
            if item['remove'][0]['success'] is True:
                output.append(" ".join(["Volume", item['remove'][0]['id'], "successfully deleted.",
                                        formatter.green('OK')]))
            else:
                output.append(" ".join([item['remove'][0]['ex'], formatter.red('Failed')]))
        return "\n".join(output)


def client_control_job(action, clients, volumes):
    get_api_ready()
    for client in clients:
        for volume in volumes:
            payload = {
                '_id': client,
                'controlJobs': [{
                    'uuid': volume,
                    'control': CONTROL_JOBS[action]
                }]
            }
            api_return = nvmesh.set_control_jobs(payload)
            if api_return == 'null':
                print(" ".join([action.title(), "volume %s on client %s:" % (volume, client), formatter.green("OK")]))
            else:
                print(" ".join([action.title(), "volume %s on client %s:" % (volume, client), formatter.red("Failed"),
                                str(api_return)]))


class NvmeshShell(Cmd):

    def __init__(self):
        Cmd.__init__(self, use_ipython=True)

    prompt = "\033[1;34mnvmesh #\033[0m "
    show_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    show_parser.add_argument('nvmesh_object', choices=['cluster', 'targets', 'clients', 'volumes', 'manager',
                                                       'sshuser', 'apiuser', 'vpgs', 'driveclasses', 'targetclasses',
                                                       'hosts'],
                             help='Define/specify the scope or the NVMesh object you want to list or view.')
    show_parser.add_argument('-c', '--classes', nargs='+', required=False,
                             help='A single or a space separated list of NVMesh drives or target classes.')
    show_parser.add_argument('-d', '--details', required=False, action='store_const', const=True,
                             help='Show more details.')
    show_parser.add_argument('-j', '--json', required=False, action='store_const', const=True,
                             help='Format output as JSON.')
    show_parser.add_argument('-s', '--servers', nargs='+', required=False,
                             help='Space separated list or view NVMesh objects of a single server or a list of servers.')
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
The 'list sub-command allows output in a table, tabulator separated value or JSON format. E.g 'list targets' will list all targets. In case you want to see the properties of only one or just a few you need to use the '-s' or '--server' option to specify single or a list of servers/targets. E.g. 'list targets -s target1 target2'"""
        mgmt.get_management_server()
        user.get_api_user()
        if args.nvmesh_object == 'targets':
            self.poutput(show_targets(args.details, args.tsv, args.json, args.servers, args.short_names))
        elif args.nvmesh_object == 'clients':
            self.poutput(show_clients(args.tsv, args.json, args.servers, args.short_names))
        elif args.nvmesh_object == 'volumes':
            self.poutput(show_volumes(args.details, args.tsv, args.json, args.volumes, args.short_names))
        elif args.nvmesh_object == 'sshuser':
            self.poutput(user.get_ssh_user())
        elif args.nvmesh_object == 'apiuser':
            self.poutput(user.get_api_user())
        elif args.nvmesh_object == 'manager':
            self.poutput(mgmt.server)
        elif args.nvmesh_object == 'cluster':
            self.poutput(show_cluster(args.tsv, args.json))
        elif args.nvmesh_object == 'vpgs':
            self.poutput(show_vpgs(args.tsv, args.json, args.vpgs))
        elif args.nvmesh_object == 'driveclasses':
            self.poutput(show_drive_classes(args.details, args.tsv, args.json, args.classes))
        elif args.nvmesh_object == 'targetclasses':
            self.poutput(show_target_classes(args.tsv, args.json, args.classes))
        elif args.nvmesh_object == 'hosts':
            self.poutput("\n".join(hosts.manage_hosts("get", None)))

    add_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    add_parser.add_argument('nvmesh_object', choices=['hosts', 'volume'],
                            help='Add hosts to this shell environment or add/creat new NVMesh volumes.')
    add_parser.add_argument('-r', '--raid_level', nargs=1, required=False,
                            help='The RAID level of the volume. Options: lvm, 0, 1, 10')
    add_parser.add_argument('-v', '--vpg', nargs=1, required=False,
                            help='Optional - The volume provisioning group to use.')
    add_parser.add_argument('-o', '--domain', nargs=1, required=False,
                            help='Optional - Domain/awareness information to use.')
    add_parser.add_argument('-D', '--description', nargs=1, required=False,
                            help='Optional - Volume description')
    add_parser.add_argument('-l', '--limit-by-disk', nargs='+', required=False,
                            help='Optional - Limit volume allocation to specific drives.')
    add_parser.add_argument('-L', '--limit-by-targets', nargs='+', required=False,
                            help='Optional - Limit volume allocation to specific target nodes.')
    add_parser.add_argument('-n', '--name', nargs=1, required=False,
                            help='Name of the volume, must be unique, will be the ID of the volume.')
    add_parser.add_argument('-N', '--number-of-mirrors', nargs=1, required=False,
                            help='Number of mirrors to use.')
    add_parser.add_argument('-c', '--count', nargs=1, required=False,
                            help='Number of volumes to create and add. 100 Max.')
    add_parser.add_argument('-t', '--target-class', nargs='+', required=False,
                            help='Optional - Limit volume allocation to specific target classes.')
    add_parser.add_argument('-d', '--drive-class', nargs='+', required=False,
                            help='Optional - Limit volume allocation to specific drive classes.')
    add_parser.add_argument('-w', '--stripe-width', nargs=1, required=False,
                            help='Number of disks to use. Required for R0 and R10.')
    add_parser.add_argument('-s', '--servers', nargs='+', required=False,
                            help='Specify a single server or a space separated list of servers.')
    add_parser.add_argument('-S', '--size', nargs=1, required=False,
                            help='Specify the size of the new volume. The volumes size value is base*2/binary. '
                                 'Example: -S 12GB or 12GiB will create a volume with a size of 12884901888 bytes.'
                                 'Some valid input formats samples: xGB, x GB, x gigabyte, x GiB or xG')

    @with_argparser(add_parser)
    def do_add(self, args):
        """The 'add' sub-command will let you add nvmesh objects to your cluster or nvmesh-shell runtime environment. E.g. 'add hosts' will add host entries to your nvmesh-shell environment while 'add volume' will create and add a new volume to the NVMesh cluster."""
        action = "add"
        if args.nvmesh_object == 'hosts':
            hosts.manage_hosts(action, args.servers)
        elif args.nvmesh_object == 'volume':
            if args.name is None:
                print formatter.yellow(
                    "Volume name missing! Use the -n argument to provide a volume name")
                return
            if args.count is not None:
                if int(args.count[0]) > 100:
                    self.poutput(formatter.yellow("Count too high! The max is 100."))
                    return
                else:
                    count = 1
                    while count <= int(args.count[0]):
                        name = "".join([args.name[0], "%03d" % (count,)])
                        count = count + 1
                        self.poutput(manage_volume('create', name, args.size, args.description, args.drive_class,
                                                   args.target_class, args.limit_by_targets, args.limit_by_disk,
                                                   args.domain,
                                                   args.raid_level, args.stripe_width, args.number_of_mirrors,
                                                   args.vpg))
            else:
                self.poutput(manage_volume('create', args.name[0], args.size, args.description, args.drive_class,
                                           args.target_class, args.limit_by_targets, args.limit_by_disk, args.domain,
                                           args.raid_level, args.stripe_width, args.number_of_mirrors, args.vpg))

    delete_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    delete_parser.add_argument('nvmesh_object', choices=['hosts', 'volume'],
                               help='Add hosts/servers to this shell environment')
    delete_group = delete_parser.add_mutually_exclusive_group()
    delete_group.add_argument('-s', '--server', nargs='+',
                               help='Specify a single server or a list of servers.')
    delete_group.add_argument('-v', '--volume', nargs='+',
                               help='Specify a single volume or a space seprataed list of volumes.')

    @with_argparser(delete_parser)
    def do_delete(self, args):
        """The 'delete' sub-command will let you delete nvmesh objects in your cluster or nvmesh-shell
runtime environment. E.g. 'delete hosts' will delete host entries in your nvmesh-shell environment and 'delete volume' will delete NVMesh volumes in your NVMesh cluster."""
        action = "delete"
        if args.nvmesh_object == 'hosts':
            hosts.manage_hosts(action, args.server)
        elif args.nvmesh_object == 'volume':
            if args.volume[0] == 'all':
                volume_list = get_volume_list()
                self.poutput(manage_volume('remove', volume_list, None, None, None, None, None, None, None, None, None,
                                           None, None))
            else:
                self.poutput(manage_volume('remove', args.volume, None, None, None, None, None, None, None, None, None,
                                       None, None))

    attach_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    attach_parser.add_argument('-c', '--client', nargs='+', required=True,
                               help='Specify a single server or a space seprataed list of servers.')
    attach_parser.add_argument('-v', '--volume', nargs='+', required=True,
                               help='Specify a single volume or a space seprataed list of volumes.')

    @with_argparser(attach_parser)
    def do_attach(self, args):
        """The 'attach' sub-command will let you attach NVMesh volumes to the clients in your NVMesh cluster."""
        if args.client[0] == 'all':
            client_list = get_client_list(True)
        else:
            client_list = args.client
        if args.volume[0] == 'all':
            volume_list = get_volume_list()
        else:
            volume_list = args.volume
        self.poutput(client_control_job('attach', client_list, volume_list))

    detach_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    detach_parser.add_argument('-c', '--client', nargs='+', required=True,
                               help='Specify a single server or a space seprataed list of servers.')
    detach_parser.add_argument('-v', '--volume', nargs='+', required=True,
                               help='Specify a single volume or a space seprataed list of volumes.')

    @with_argparser(detach_parser)
    def do_detach(self, args):
        """The 'detach' sub-command will let you detach NVMesh volumes in your NVMesh cluster."""
        if args.client[0] == 'all':
            client_list = get_client_list(True)
        else:
            client_list = args.client
        if args.volume[0] == 'all':
            volume_list = get_volume_list()
        else:
            volume_list = args.volume
        self.poutput(client_control_job('detach', client_list, volume_list))

    check_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    check_parser.add_argument('nvmesh_object', choices=['clients', 'targets', 'managers', 'cluster'],
                              help='Specify where you want to check the NVMesh services status.')
    check_parser.add_argument('-d', '--details', required=False, action='store_const', const=True,
                              help='Show detailed service information.')
    check_parser.add_argument('-p', '--prefix', required=False, action='store_const', const=True,
                              help='Adds the host name at the beginning of each line. This helps to identify the '
                                   'content when piping into a grep or similar')
    check_parser.add_argument('-P', '--parallel', required=False, action='store_const', const=True,
                              help='Check the hosts/servers in parallel.')
    check_parser.add_argument('-s', '--servers', nargs='+', required=False,
                              help='Specify a single or a space separated list of managers, targets or clients.')

    @with_argparser(check_parser)
    def do_check(self, args):
        """The 'check' sub-command checks and let you list the status of the actual NVMesh services running in your cluster. It is using SSH connectivity to the NVMesh managers, clients and targets to verify the service status. E.g. 'check targets' will check the NVMesh target services throughout the cluster."""
        user.get_ssh_user()
        user.get_api_user()
        mgmt.get_management_server()
        action = "check"
        if args.nvmesh_object == 'targets':
            self.poutput(manage_nvmesh_service('target', args.details, args.servers, action, args.prefix,
                                               args.parallel))
        elif args.nvmesh_object == 'clients':
            self.poutput(manage_nvmesh_service('client', args.details, args.servers, action, args.prefix,
                                               args.parallel))
        elif args.nvmesh_object == 'managers':
            self.poutput(manage_nvmesh_service('mgr', args.details, args.servers, action, args.prefix,
                                               args.parallel))
        elif args.nvmesh_object == 'cluster':
            manage_cluster(args.details, action, args.prefix)

    stop_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    stop_parser.add_argument('nvmesh_object', choices=['clients', 'targets', 'managers', 'cluster', 'mcm'],
                             help='Specify the NVMesh service type you want to top.')
    stop_parser.add_argument('-d', '--details', required=False, action='store_const', const=True,
                             help='List and view the service details.')
    stop_parser.add_argument('-g', '--graceful', nargs=1, required=False, default=True, choices=['True', 'False'],
                             help="Graceful stop of all NVMesh targets in the cluster."
                                  " The default is set to 'True'")
    stop_parser.add_argument('-p', '--prefix', required=False, action='store_const', const=True,
                             help='Adds the host name at the beginning of each line. This helps to identify the '
                                  'content when piping into a grep or similar')
    stop_parser.add_argument('-P', '--parallel', required=False, action='store_const', const=True,
                             help='Stop the NVMesh services in parallel.')
    stop_parser.add_argument('-s', '--servers', nargs='+', required=False,
                             help='Specify a single or a space separated list of managers, targets or clients.')

    @with_argparser(stop_parser)
    def do_stop(self, args):
        """The 'stop' sub-command will stop the selected NVMesh services on all managers, targets and clients.
Or it will stop the entire NVMesh cluster. It uses SSH connectivity to manage the NVMesh services.
E.g. 'stop clients' will stop all the NVMesh clients throughout the cluster."""
        user.get_ssh_user()
        user.get_api_user()
        mgmt.get_management_server()
        action = "stop"
        if args.nvmesh_object == 'targets':
            self.poutput(manage_nvmesh_service('target', args.details, args.servers, action, args.prefix,
                                               args.parallel))
        elif args.nvmesh_object == 'clients':
            self.poutput(manage_nvmesh_service('client', args.details, args.servers, action, args.prefix,
                                               args.parallel))
        elif args.nvmesh_object == 'managers':
            self.poutput(manage_nvmesh_service('mgr', args.details, args.servers, action, args.prefix,
                                               args.parallel))
        elif args.nvmesh_object == 'cluster':
            manage_cluster(args.details, action, args.prefix)
        elif args.nvmesh_object == 'mcm':
            manage_mcm(args.server, action)

    start_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    start_parser.add_argument('nvmesh_object', choices=['clients', 'targets', 'managers', 'cluster', 'mcm'],
                              help='Specify the NVMesh service type you want to start.')
    start_parser.add_argument('-d', '--details', required=False, action='store_const', const=True,
                              help='List and view the service details.')
    start_parser.add_argument('-p', '--prefix', required=False, action='store_const', const=True,
                              help='Adds the host name at the beginning of each line. This helps to identify the '
                                   'content when piping into a grep or similar')
    start_parser.add_argument('-P', '--parallel', required=False, action='store_const', const=True,
                              help='Start the NVMesh services on the hosts/servers in parallel.')
    start_parser.add_argument('-s', '--servers', nargs='+', required=False,
                              help='Specify a single or a space separated list of servers.')

    @with_argparser(start_parser)
    def do_start(self, args):
        """The 'start' sub-command will start the selected NVMesh services on all managers, targets and clients.
Or it will start the entire NVMesh cluster. It uses SSH connectivity to manage the NVMesh services.
E.g. 'start cluster' will start all the NVMesh services throughout the cluster."""
        user.get_ssh_user()
        user.get_api_user()
        mgmt.get_management_server()
        action = "start"
        if args.nvmesh_object == 'targets':
            self.poutput(manage_nvmesh_service('target', args.details, args.servers, action, args.prefix,
                                               args.parallel))
        elif args.nvmesh_object == 'clients':
            self.poutput(manage_nvmesh_service('client', args.details, args.servers, action, args.prefix,
                                               args.parallel))
        elif args.nvmesh_object == 'managers':
            self.poutput(manage_nvmesh_service('mgr', args.details, args.servers, action, args.prefix,
                                               args.parallel))
        elif args.nvmesh_object == 'cluster':
            manage_cluster(args.details, action, args.prefix)
        elif args.nvmesh_object == 'mcm':
            manage_mcm(args.server, action)

    restart_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    restart_parser.add_argument('nvmesh_object', choices=['clients', 'targets', 'managers', 'cluster', 'mcm'],
                                help='Specify the NVMesh service which you want to restart.')
    restart_parser.add_argument('-d', '--details', required=False, action='store_const', const=True,
                                help='List and view the service details.')
    restart_parser.add_argument('-g', '--graceful', nargs=1, required=False, default=True, choices=['True', 'False'],
                                help='Restart with a graceful stop of the targets in the cluster.'
                                     'The default is set to True')
    restart_parser.add_argument('-p', '--prefix', required=False, action='store_const', const=True,
                                help='Adds the host name at the beginning of each line. This helps to identify the '
                                     'content when piping into a grep or similar')
    restart_parser.add_argument('-P', '--parallel', required=False, action='store_const', const=True,
                                help='Restart the NVMesh services on the hosts/servers in parallel.')
    restart_parser.add_argument('-s', '--servers', nargs='+', required=False,
                                help='Specify a single or a space separated list of servers.')

    @with_argparser(restart_parser)
    def do_restart(self, args):
        """The 'restart' sub-command will restart the selected NVMesh services on all managers, targets and clients.
Or it will restart the entire NVMesh cluster. It uses SSH connectivity to manage the NVMesh services.
E.g. 'restart managers' will restart the NVMesh management service."""
        user.get_ssh_user()
        user.get_api_user()
        mgmt.get_management_server()
        action = 'restart'
        if args.nvmesh_object == 'targets':
            self.poutput(manage_nvmesh_service('target', args.details, args.servers, action, args.prefix,
                                               args.parallel))
        elif args.nvmesh_object == 'clients':
            self.poutput(manage_nvmesh_service('client', args.details, args.servers, action, args.prefix,
                                               args.parallel))
        elif args.nvmesh_object == 'managers':
            self.poutput(manage_nvmesh_service('mgr', args.details, args.servers, action, args.prefix,
                                               args.parallel))
        elif args.nvmesh_object == 'mcm':
            manage_mcm(args.server, action)
        elif args.nvmesh_object == 'cluster':
            manage_cluster(args.details, action, args.prefix)

    define_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    define_parser.add_argument('nvmesh_object', choices=['manager', 'sshuser', 'sshpassword', 'apiuser', 'apipassword'],
                               help='Specify the NVMesh shell runtime variable you want to define.')
    define_parser.add_argument('-t', '--persistent', required=False, action='store_const', const=True,
                               help='Define/Set the NVMesh runtime variable persistently.')
    define_parser.add_argument('-p', '--password', nargs=1, required=False,
                               help='The password for the user to be used.')
    define_parser.add_argument('-u', '--user', nargs=1, required=False,
                               help='The username name for the user to be used.')
    define_parser.add_argument('-s', '--server', nargs='+', required=False,
                               help='The NVMesh management server name')

    @with_argparser(define_parser)
    def do_define(self, args):
        """The 'define' sub-command defines/sets the shell runtime variables. It can be used to set them temporarily or
persistently. Please note that in its current version allows to set only one NVMesh manager. If you try to provide a
list it will use the first manager name of that list.
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

    runcmd_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    runcmd_parser.add_argument('scope', choices=['clients', 'targets', 'managers', 'cluster', 'hosts'],
                               help='Specify the scope where you want to run the command.')
    runcmd_parser.add_argument('-c', '--command', nargs='+', required=True,
                               help='The command you want to run on the servers. Use quotes if the command needs to run'
                                    ' with flags by itself, like: runcmd cluster -c "uname -a"')
    runcmd_parser.add_argument('-p', '--prefix', required=False, action='store_const', const=True,
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
        user.get_ssh_user()
        user.get_api_user()
        mgmt.get_management_server()
        self.poutput(run_command(args.command, args.scope, args.prefix, args.parallel, args.servers))


def start_shell():
    reload(sys)
    sys.setdefaultencoding('utf-8')
    history_file = os.path.expanduser('~/.nvmesh_shell_history')
    if not os.path.exists(history_file):
        with open(history_file, "w") as history:
            history.write("")
    readline.read_history_file(history_file)
    atexit.register(readline.write_history_file, history_file)
    shell = NvmeshShell()
    if len(sys.argv) > 1:
        shell.onecmd(' '.join(sys.argv[1:]))
    else:
        shell.cmdloop('''
Copyright (c) 2018 Excelero, Inc. All rights reserved.

This program comes with ABSOLUTELY NO WARRANTY; for licensing and warranty details type 'license'.
This is free software, and you are welcome to redistribute it under certain conditions; type 'license' for details.

Starting the NVMesh shell ...''')


if __name__ == '__main__':
    start_shell()
