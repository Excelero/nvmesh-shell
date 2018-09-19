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

import logger
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
import nvmesh_api
import time
import urllib3
from multiprocessing import Pool
import dateutil.parser
import re

version = '39'

RAID_LEVELS = {
    'lvm': 'LVM/JBOD',
    '0': 'Striped RAID-0',
    '1': 'Mirrored RAID-1',
    '10': 'Striped & Mirrored RAID-10'
}

NVME_VENDORS = {
    '0x1344': 'Micron',
    '0x15b7': 'SanDisk',
    '0x1179': 'Toshiba',
    '0x144D': 'Samsung'
}

WARNINGS = {
    'delete_volume': 'This operation will DESTROY ALL DATA on the volume selected and is IRREVERSIBLE.\nDo you want to continue? [Yes|No]: ',
    'format_drive': 'This operation will DESTROY ALL DATA on the drives and is IRREVERSIBLE.\nDo you want to continue? [Yes|No]: ',
    'force_detach_volume': 'This operation will immediately HALT ALL I/O and will impact any running applications expecting it to be available. It is recommended that all applications and/or file systems using this volume be stopped/un-mounted prior to issuing the command.\nDo you want to continue? [Yes|No]: ',
    'stop_nvmesh_client': 'This operation will HALT ALL I/O TO ALL VOLUMES in use by THE SELECTED CLIENT. It is recommended that all applications and/or file systems supported by NVMesh volumes on the clients be stopped/un-mounted prior to issuing the command.\nDo you want to continue? [Yes|No]: ',
    'stop_nvmesh_target': 'This operation will make any UNPROTECTED VOLUMES supported by drives in the selected targets IMMEDIATELY UNAVAILABLE. Any PROTECTED VOLUMES will become IMMEDIATELY DEGRADED until services are restarted or volumes are rebuilt to alternate drives in another target.\nDo you want to continue? [Yes|No]: ',
    'stop_nvmesh_manager': 'This operation will halt the running instance of NVMesh Management on the selected servers. If Management is deployed as a stand-alone instance, or this is the last running HA instance, further changes to NVMesh cluster volumes, clients, and targets will be unavailable until Management is restarted on at least one node.\nDo you want to continue? [Yes|No]: ',
    'stop_cluster': 'ALL NVMesh resources will become UNAVAILABLE and ALL IO will stop. It is recommended that all applications and/or file systems using any resource out of this cluster be stopped/un-mounted prior to issuing the command.\nDo you want to continue? [Yes|No]: '
}


class ArgsUsageOutputFormatter(argparse.HelpFormatter):
    def _format_usage(self, usage, actions, groups, prefix):
        if prefix is None:
            prefix = 'usage: '
        if usage is not None:
            usage = usage % dict(prog=self._prog)
        elif usage is None and not actions:
            usage = '%(prog)s' % dict(prog=self._prog)
        elif usage is None:
            prog = '%(prog)s' % dict(prog=self._prog)
            action_usage = self._format_actions_usage(actions, groups)
            usage = ' '.join([s for s in [prog, action_usage] if s])
        return '%s%s\n\n' % (prefix, usage)


class OutputFormatter:
    def __init__(self):
        self.text = None

    @staticmethod
    def print_green(text):
        print('\033[92m' + text + '\033[0m')

    @staticmethod
    def print_yellow(text):
        print('\033[33m' + text + '\033[0m')

    @staticmethod
    def print_red(text):
        print('\033[31m' + text + '\033[0m')

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
        print("[ " + host.strip() + " ]\t.\t.\t." + text)
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
    def add_line_prefix(prefix, text, short):
        if short:
            text_lines = [' '.join([prefix.split('.')[0], line]) for line in text.splitlines()]
        else:
            text_lines = [' '.join([prefix, line]) for line in text.splitlines()]
        return '\n'.join(text_lines)


class Hosts:
    def __init__(self):
        self.host_list = []
        self.host_file = os.path.expanduser('~/.nvmesh_hosts')
        self.test_host_connection_test_result = None
        self.formatter = OutputFormatter()
        self.host_delete_list = []

    def manage_hosts(self, action, hosts_list, silent):
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
                if silent:
                    return None
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

    def get_management_server_list(self):
        if os.path.isfile(self.server_file):
            self.server = [server.strip() for server in open(self.server_file, 'r').readlines()]
            return sorted(self.server)
        else:
            formatter.print_yellow("No API management server defined yet!")
            server_list = raw_input(
                "Provide a space separated list, min. one, of the NVMesh manager server names: ").split(" ")
            self.save_management_server(server_list)
            return sorted(server_list)

    def save_management_server(self, server_list):
        open(self.server_file, 'w').write("\n".join(server_list))
        return


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
            logger.log('critical', e.message)
            formatter.print_red(e.message)
            pass
        if self.SSH_secrets is None:
            formatter.print_yellow("SSH user credentials not set yet!")
            self.SSH_user_name = raw_input("Provide the root level SSH user name: ")
            self.SSH_password = getpass.getpass("Please provide the SSH password: ")
            self.save_ssh_user()
            self.get_ssh_user()
            return self.SSH_user_name
        else:
            self.SSH_user_name = self.SSH_secrets[0]
            self.SSH_password = base64.b64decode(self.SSH_secrets[1])
            return self.SSH_user_name, self.SSH_password

    def get_api_user(self):
        try:
            self.API_secrets = open(self.API_secrets_file, 'r').read().split(' ')
        except Exception, e:
            logger.log('critical', e.message)
            formatter.print_red(e.message)
            pass
        if self.API_secrets is None:
            formatter.print_yellow("API user credentials not set yet!")
            self.API_user_name = raw_input("Provide the root level API user name: ")
            self.API_password = getpass.getpass("Please provide the API password: ")
            self.save_api_user()
            self.get_api_user()
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
        self.ssh_user_name = user.get_ssh_user()[0]
        self.ssh_password = user.get_ssh_user()[1]

    def test_ssh_connection(self, host_list):
        if host_list is None:
            host_list = Hosts().manage_hosts('get', None, True)
        if host_list is None:
            host_list = get_client_list(False)
            host_list.extend(get_target_list(short=True))
            host_list.extend(get_manager_list(short=True))
            host_list = set(host_list)
        for host in host_list:
            try:
                self.ssh.connect(
                    host, username=user.SSH_user_name, password=user.SSH_password, timeout=5, port=self.ssh_port)
                self.ssh.close()
                print(" ".join(['Connection to %s' % host, formatter.green('OK')]))
            except Exception, e:
                print(" ".join(['Connection to %s' % host, formatter.red('Failed:'), e.message]))
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
            self.ssh.connect(host, username=self.ssh_user_name, password=self.ssh_password, timeout=5,
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
            logger.log("critical", e)
            print formatter.print_red("Couldn't execute command %s on %s! %s" % (remote_command, host, e.message))

    def execute_remote_command(self, host, remote_command):
        try:
            self.ssh.connect(host.strip(), username=user.SSH_user_name, password=user.SSH_password, timeout=5,
                             port=self.ssh_port)
            stdin, stdout, stderr = self.ssh.exec_command(remote_command)
            return stdout.channel.recv_exit_status(), "Success - OK"
        except Exception, e:
            logger.log("critical", e)
            print formatter.print_red("Couldn't execute command %s on %s!" % (remote_command, host))
            return

    def check_if_service_is_running(self, host, service):
        try:
            cmd_output = self.execute_remote_command(host, "/etc/init.d/%s" % service)
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
    user.get_api_user()
    nvmesh.user_name = user.API_user_name
    nvmesh.password = user.API_password
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    manager_list = mgmt.get_management_server_list()
    for manager in manager_list:
        nvmesh.server = manager.strip()
        try:
            nvmesh.login()
            return 0
        except Exception, e:
            if len(manager_list) < 2:
                message = "\n".join(["Cannot log into management server %s!" % manager.strip(),
                                     "Currently defined servers in the cli tool:",
                                     "\n".join(sorted(open(ManagementServer().server_file).read().splitlines()))])
                logger.log("critical", "\n".join([message, str(e.message)]))
                print(formatter.red(message))
                return 1
            else:
                if manager_list.index(manager) + 1 == len(manager_list):
                    message = "\n".join(["Cannot log into any management server as defined in the nvmesh cli list! "
                                         "Use 'define manager' to update and correct the list of management servers to "
                                         "be used by the cli tool.", "Currently defined servers in the cli tool:",
                                         "\n".join(sorted(open(ManagementServer().server_file).read().splitlines()))])
                    logger.log("critical", "\n".join([message, str(e.message)]))
                    print(formatter.red(message))
                    return 1
                message = "Cannot log into management server %s, Trying the next one in the list." % manager.strip()
                print(formatter.yellow(message))
                logger.log("warning", "\n".join([message, str(e.message)]))
                continue


def show_cluster(csv_format, json_format):
    if get_api_ready() == 0:
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
            return format_smart_table(cluster_list,
                                      ['Total Servers', 'Offline Servers', 'Total Clients', 'Offline Clients',
                                       'Volumes', 'Total Capacity', 'Available Space'])


def show_targets(details, csv_format, json_format, server, short):
    if get_api_ready() == 0:
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
                if target["health"] == "healthy":
                    health = formatter.green(formatter.bold("Healthy ")) + u'\u2705'
                else:
                    health = formatter.red(formatter.bold("Critical ")) + u'\u274C'
                for disk in target['disks']:
                    target_disk_list.append(disk['diskID'])
                for nic in target['nics']:
                    target_nic_list.append(nic['nicID'])
                if details is True:
                    target_list.append([target_name, health, target['version'],
                                        ' '.join(target_disk_list),
                                        ' '.join(target_nic_list)])
                else:
                    target_list.append([target_name, health, target['version']])
        if details is True:
            if csv_format is True:
                return formatter.print_tsv(target_list)
            elif json_format is True:
                formatter.print_json(target_list)
                return
            else:
                return format_smart_table(sorted(target_list),
                                          ['Target Name', 'Target Health', 'NVMesh Version', 'Target Disks',
                                           'Target NICs'])
        else:
            if csv_format is True:
                return formatter.print_tsv(target_list)
            elif json_format is True:
                return formatter.print_json(target_list)
            else:
                return format_smart_table(sorted(target_list), ['Target Name', 'Target Health', 'NVMesh Version'])


def get_target_list(short):
    if get_api_ready() == 0:
        target_json = json.loads(nvmesh.get_servers())
        target_list = []
        for target in target_json:
            if short:
                target_list.append(target['node_id'].split('.')[0])
            else:
                target_list.append(target['node_id'])
        return target_list


def get_client_list(full):
    if get_api_ready() == 0:
        clients_json = json.loads(nvmesh.get_clients())
        client_list = []
        for client in clients_json:
            if full is True:
                client_list.append(client['client_id'])
            else:
                client_list.append(client['client_id'].split('.')[0])
        return client_list


def get_volume_list():
    if get_api_ready() == 0:
        volume_json = json.loads(nvmesh.get_volumes())
        volume_list = []
        for volume in volume_json:
            volume_list.append(volume['_id'].split('.')[0])
        return volume_list


def get_drive_class_list():
    if get_api_ready() == 0:
        drive_class_list = []
        drive_class_json = json.loads(nvmesh.get_disk_classes())
        for drive_class in drive_class_json:
            drive_class_list.append(drive_class["_id"])
        return drive_class_list


def get_target_class_list():
    if get_api_ready() == 0:
        target_class_list = []
        target_class_json = json.loads(nvmesh.get_target_classes())
        for target_class in target_class_json:
            target_class_list.append(target_class["_id"])
        return target_class_list


def show_manager():
    if get_api_ready() == 0:
        manager_list = []
        manager_json = json.loads(nvmesh.get_managers())
        for manager in manager_json:
            manager_list.append([
                manager["hostname"],
                manager["ip"],
                u'\N{check mark}' if "isMe" in manager else " ",
                u'\N{check mark}' if manager["useSSL"] else " ",
                manager["port"],
                manager["outbound_socket_status"] if "isMe" not in manager else "n/a",
                manager["inbound_socket_status"] if "isMe" not in manager else "n/a"
            ])
        return format_smart_table(sorted(manager_list), [
            "Manager",
            "IP",
            "Current Connection",
            "Use SSL",
            "Port",
            "Outbound Socket",
            "Inbound Socket"])


def get_manager_list(short):
    if get_api_ready() == 0:
        manager_list = []
        manager_json = json.loads(nvmesh.get_managers())
        for manager in manager_json:
            if short:
                manager_list.append(manager["hostname"].split(".")[0])
            else:
                manager_list.append(manager["hostname"])
        return sorted(manager_list) if manager_list is not None else None


def show_clients(csv_format, json_format, server, short):
    if get_api_ready() == 0:
        clients_json = json.loads(nvmesh.get_clients())
        client_list = []
        for client in clients_json:
            if server is not None and client['client_id'].split('.')[0] not in server:
                continue
            else:
                volume_list = []
                if client["health"] == "healthy":
                    health = formatter.green(formatter.bold("Healthy ")) + u'\u2705'
                else:
                    health = formatter.red(formatter.bold("Critical ")) + u'\u274C'
                if short is True:
                    client_name = client['client_id'].split('.')[0]
                else:
                    client_name = client['client_id']
                for volume in client['block_devices']:
                    if volume['vol_status'] == 4:
                        volume_list.append(volume['name'])
                client_list.append(
                    [client_name, health, client['version'], ' '.join(sorted(set(volume_list)))])
        if csv_format is True:
            return formatter.print_tsv(client_list)
        elif json_format is True:
            return formatter.print_json(client_list)
        else:
            return format_smart_table(sorted(client_list),
                                      ['Client Name', 'Client Health', 'Client Version', 'Client Volumes'])


def show_volumes(details, csv_format, json_format, volumes, short, layout):
    if get_api_ready() == 0:
        volumes_json = json.loads(nvmesh.get_volumes())
        volumes_list = []
        for volume in volumes_json:
            remaining_dirty_bits = 0
            name = formatter.bold(volume["name"])
            if volume["health"] == "healthy":
                health = formatter.green(formatter.bold("Healthy ")) + u'\u2705'
                status = formatter.green(formatter.bold(volume["status"].capitalize()))
            elif volume["health"] == "alarm":
                health = formatter.yellow(formatter.bold("Alarm !!"))
                status = formatter.yellow(formatter.bold(volume["status"].capitalize()))
            else:
                health = formatter.red(formatter.bold("Critical ")) + u'\u274C'
                status = formatter.red(formatter.bold(volume["status"].capitalize()))

            if volumes is not None and volume['name'] not in volumes:
                continue
            else:
                if 'stripeWidth' in volume:
                    stripe_width = volume['stripeWidth']
                else:
                    stripe_width = None
                if 'domain' in volume:
                    awareness_domain = volume['domain']
                else:
                    awareness_domain = None
                if 'serverClasses' in volume:
                    if len(volume['serverClasses']) > 0:
                        target_classes_list = volume['serverClasses']
                    else:
                        target_classes_list = None
                else:
                    target_classes_list = None

                if 'diskClasses' in volume:
                    if len(volume['diskClasses']) > 0:
                        drive_classes_list = volume['diskClasses']
                    else:
                        drive_classes_list = None
                else:
                    drive_classes_list = None

                target_list = []
                target_disk_list = []
                chunk_count = 0
                volume_layout_list = []

                if layout:
                    for chunk in volume['chunks']:
                        for praid in chunk['pRaids']:
                            for segment in praid['diskSegments']:
                                volume_layout_list.append([str(chunk_count),
                                                           str(praid['stripeIndex']),
                                                           str(segment['pRaidIndex']),
                                                           segment['type'],
                                                           str(segment['lbs']) if segment['lbs'] != 0 else "n/a",
                                                           str(segment['lbe']) if segment['lbe'] != 0 else "n/a",
                                                           u'\u274C' if segment['isDead'] is True else u'\u2705',
                                                           segment['diskID'],
                                                           segment['node_id']])
                        chunk_count += 1

                for chunk in volume['chunks']:
                    for praid in chunk['pRaids']:
                        for segment in praid['diskSegments']:
                            if segment['type'] == 'raftonly':
                                continue
                            else:
                                if "remainingDirtyBits" in segment:
                                    remaining_dirty_bits = remaining_dirty_bits + segment['remainingDirtyBits']
                                target_disk_list.append(segment['diskID'])
                                if short is True:
                                    target_list.append(segment['node_id'].split('.')[0])
                                else:
                                    target_list.append(segment['node_id'])

                if details is True and not layout:
                    volumes_list.append([name,
                                         health,
                                         status,
                                         volume['RAIDLevel'],
                                         humanfriendly.format_size((int(volume['blocks']) * int(volume['blockSize'])),
                                                                   binary=True),
                                         stripe_width if stripe_width is not None else "n/a",
                                         humanfriendly.format_size((remaining_dirty_bits * 4096), binary=True),
                                         ' '.join(set(target_list)),
                                         ' '.join(set(target_disk_list)),
                                         ' '.join(target_classes_list) if target_classes_list is not None else "n/a",
                                         ' '.join(drive_classes_list) if drive_classes_list is not None else "n/a",
                                         awareness_domain if awareness_domain is not None else "n/a"])

                elif details is True and layout:
                    volumes_list.append([name,
                                         health,
                                         status,
                                         volume['RAIDLevel'],
                                         humanfriendly.format_size((int(volume['blocks']) * int(volume['blockSize'])),
                                                                   binary=True),
                                         stripe_width if stripe_width is not None else "n/a",
                                         humanfriendly.format_size((remaining_dirty_bits * 4096), binary=True),
                                         ' '.join(set(target_list)),
                                         ' '.join(set(target_disk_list)),
                                         ' '.join(target_classes_list) if target_classes_list is not None else "n/a",
                                         ' '.join(drive_classes_list) if drive_classes_list is not None else "n/a",
                                         awareness_domain if awareness_domain is not None else "n/a",
                                         format_smart_table(volume_layout_list, ["Chunk",
                                                                                 "Stripe",
                                                                                 "Segment",
                                                                                 "Type",
                                                                                 "LBA Start",
                                                                                 "LBA End",
                                                                                 "Status",
                                                                                 "Disk ID",
                                                                                 "Last Known Target"])])
                else:
                    volumes_list.append([name,
                                         health,
                                         status,
                                         volume['RAIDLevel'],
                                         humanfriendly.format_size((int(volume['blocks']) * int(volume['blockSize'])),
                                                                   binary=True),
                                         stripe_width if stripe_width is not None else "n/a",
                                         humanfriendly.format_size((remaining_dirty_bits * 4096), binary=True)])
        if details is True and not layout:
            if csv_format is True:
                return formatter.print_tsv(volumes_list)
            elif json_format is True:
                return formatter.print_json(volumes_list)
            else:
                return format_smart_table(sorted(volumes_list),
                                          ['Volume Name',
                                           'Volume Health',
                                           'Volume Status',
                                           'Volume Type',
                                           'Volume Size',
                                           'Stripe Width',
                                           'Dirty Bits',
                                           'Target Names',
                                           'Target Disks',
                                           'Target Classes',
                                           'Drive Classes',
                                           'Awareness/Domain'])
        elif details is True and layout:
            if csv_format is True:
                return formatter.print_tsv(volumes_list)
            elif json_format is True:
                return formatter.print_json(volumes_list)
            else:
                return format_smart_table(sorted(volumes_list),
                                          ['Volume Name',
                                           'Volume Health',
                                           'Volume Status',
                                           'Volume Type',
                                           'Volume Size',
                                           'Stripe Width',
                                           'Dirty Bits',
                                           'Target Names',
                                           'Target Disks',
                                           'Target Classes',
                                           'Drive Classes',
                                           'Awareness/Domain',
                                           'Volume Layout'])
        else:
            if csv_format is True:
                return formatter.print_tsv(volumes_list)
            elif json_format is True:
                return formatter.print_json(volumes_list)
            else:
                return format_smart_table(sorted(volumes_list),
                                          ['Volume Name',
                                           'Volume Health',
                                           'Volume Status',
                                           'Volume Type',
                                           'Volume Size',
                                           'Stripe Width',
                                           'Dirty Bits'])


def show_vpgs(csv_format, json_format, vpgs):
    if get_api_ready() == 0:
        vpgs_json = json.loads(nvmesh.get_vpgs())
        vpgs_list = []
        for vpg in vpgs_json:
            server_classes_list = []
            disk_classes_list = []
            if vpgs is not None and vpg['name'] not in vpgs:
                continue
            else:
                vpg_description = vpg['description'] if 'description' in vpg else " "
                if 'stripeWidth' not in vpg:
                    vpg_stripe_width = ''
                else:
                    vpg_stripe_width = vpg['stripeWidth']
                for disk_class in vpg['diskClasses']:
                    disk_classes_list.append(disk_class)
                for server_class in vpg['serverClasses']:
                    server_classes_list.append(server_class)

            vpgs_list.append(
                [vpg['name'], vpg_description, vpg['RAIDLevel'], vpg_stripe_width,
                 humanfriendly.format_size(vpg['capacity'], binary=True),
                 '; '.join(disk_classes_list), '; '.join(server_classes_list)])
        if csv_format is True:
            return formatter.print_tsv(vpgs_list)
        elif json_format is True:
            return formatter.print_json(vpgs_list)
        else:
            return format_smart_table(vpgs_list, ['VPG Name',
                                                  'Description',
                                                  'RAID Level',
                                                  'Stripe Width',
                                                  'Reserved Capacity',
                                                  'Disk Classes',
                                                  'Target Classes'])


def show_drive_classes(details, csv_format, json_format, classes):
    if get_api_ready() == 0:
        drive_classes_json = json.loads(nvmesh.get_disk_classes())
        drive_class_list = []
        for drive_class in drive_classes_json:
            drive_model_list = []
            drive_target_list = []
            domain_list = []
            if classes is not None and drive_class['_id'] not in classes:
                continue
            else:
                if 'domains' in drive_class:
                    for domain in drive_class['domains']:
                        domain_list.append("scope:" + domain['scope'] + " identifier:" + domain['identifier'])
                else:
                    domain_list = None
                for disk in drive_class['disks']:
                    drive_model_list.append(disk['model'])
                    if disk["disks"]:
                        for drive in disk['disks']:
                            if details is True:
                                drive_target_list.append(' '.join([drive['diskID'], drive['node_id']]))
                            else:
                                drive_target_list.append(drive['diskID'])
                    else:
                        drive_target_list = []
                drive_class_list.append([drive_class['_id'],
                                         '; '.join(drive_model_list),
                                         '; '.join(drive_target_list),
                                         '; '.join(domain_list) if domain_list is not None else "n/a"])
        if csv_format is True:
            return formatter.print_tsv(drive_class_list)
        elif json_format is True:
            return formatter.print_json(drive_class_list)
        else:
            return format_smart_table(drive_class_list, ['Drive Class',
                                                         'Drive Model',
                                                         'Drive Details',
                                                         'Awareness/Domains'])


def show_logs(all_logs):
    if get_api_ready() == 0:
        logs_list = []
        logs_json = json.loads(nvmesh.get_logs(all_logs))
        for log_entry in logs_json:
            if log_entry["level"] == "ERROR":
                logs_list.append(
                    "\t".join([str(dateutil.parser.parse(log_entry["timestamp"])), formatter.red(log_entry["level"]),
                               log_entry["message"]]))
            elif log_entry["level"] == "WARNING":
                logs_list.append(
                    "\t".join([str(dateutil.parser.parse(log_entry["timestamp"])), formatter.yellow(log_entry["level"]),
                               log_entry["message"]]).strip())
            else:
                logs_list.append(
                    "\t".join(
                        [str(dateutil.parser.parse(log_entry["timestamp"])), log_entry["level"],
                         log_entry["message"]]).strip())
        return "\n".join(logs_list)


def show_target_classes(csv_format, json_format, classes):
    if get_api_ready() == 0:
        target_classes_json = json.loads(nvmesh.get_target_classes())
        target_classes_list = []
        for target_class in target_classes_json:
            if classes is not None and target_class['_id'] not in classes:
                continue
            else:
                target_nodes = []
                domain_list = []
                if 'domains' in target_class:
                    for domain in target_class['domains']:
                        domain_list.append("scope:" + domain['scope'] + " identifier:" + domain['identifier'])
                else:
                    domain_list = None
                if 'description' not in target_class:
                    target_class_description = "n/a"
                else:
                    target_class_description = target_class['description']
                for node in target_class['targetNodes']:
                    target_nodes.append(node)

            target_classes_list.append([target_class['name'],
                                        target_class_description,
                                        '; '.join(target_nodes),
                                        '; '.join(domain_list) if domain_list is not None else "n/a"])
        if csv_format is True:
            return formatter.print_tsv(target_classes_list)
        elif json_format is True:
            return formatter.print_json(target_classes_list)
        else:
            return format_smart_table(target_classes_list, ['Target Class',
                                                            'Description',
                                                            'Target Nodes',
                                                            'Awareness/Domains'])


def count_active_targets():
    active_targets = 0
    for target in get_target_list(short=True):
        ssh = SSHRemoteOperations()
        ssh_return = ssh.return_remote_command_std_output(target, "/etc/init.d/nvmeshtarget status")
        if ssh_return[0] == 0:
            active_targets += 1
    return active_targets


def parse_domain_args(args_list):
    if args_list is None:
        return None
    else:
        domain_list = []
        domain_dict = {}
        for line in args_list:
            domain_dict["scope"] = line.split("&")[0].split(":")[1]
            domain_dict["identifier"] = line.split("&")[1].split(":")[1]
            domain_list.append(domain_dict)
        return domain_list


def parse_drive_args(args_drive_list):
    if args_drive_list is None:
        return None
    drive_list = []
    for drive in args_drive_list:
        drive_list.append(
            {
                "diskID": drive.split(":")[0],
                "node_id": drive.split(":")[1].strip()
            }
        )
    return drive_list


def manage_nvmesh_service(scope, details, servers, action, prefix, parallel, graceful):
    output = []
    ssh = SSHRemoteOperations()
    host_list = []
    ssh_return = []

    if servers is not None:
        host_list = set(servers)

    else:
        if scope == 'cluster':
            host_list = get_target_list(short=True)
            host_list.extend(get_client_list(False))
            host_list.extend(mgmt.get_management_server_list())
        if scope == 'target':
            host_list = get_target_list(short=True)
        if scope == 'client':
            host_list = get_client_list(False)
        if scope == 'mgr':
            if action is not "start":
                host_list = get_manager_list(short=True)
            else:
                host_list = ManagementServer().get_management_server_list()

    if scope == "target":
        if action == "stop" and servers is None and graceful:
            nvmesh.target_cluster_shutdown({"control": "shutdownAll"})
            print("\n".join(["Shutting down the NVMesh target services in the cluster.", "Please wait..."]))
            while count_active_targets() != 0:
                time.sleep(5)
            print(" ".join(["All target services shut down.", formatter.green("OK")]))
            return

    if parallel:
        if host_list and len(host_list) > 0:
            process_pool = Pool(len(set(host_list)))
        else:
            return
        parallel_execution_map = []
        for host in set(host_list):
            if action == "check":
                parallel_execution_map.append([host, "/etc/init.d/nvmesh%s status" % scope])
            elif action == "start":
                parallel_execution_map.append([host, "/etc/init.d/nvmesh%s start" % scope])
            elif action == "stop":
                parallel_execution_map.append([host, "/etc/init.d/nvmesh%s stop" % scope])
            elif action == "restart":
                parallel_execution_map.append([host, "/etc/init.d/nvmesh%s restart" % scope])

        command_return_list = process_pool.map(run_parallel_ssh_command, parallel_execution_map)
        process_pool.close()
        for command_return in command_return_list:
            try:
                if command_return[1][0] == 0:
                    if details is True:
                        output.append(formatter.bold(" ".join([command_return[0], action.capitalize(),
                                                               formatter.green('OK')])))
                        if prefix is True:
                            output.append(formatter.add_line_prefix(command_return[0], (
                                command_return[1][1][:command_return[1][1].rfind('\n')]), True) + "\n")
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
                                command_return[1][1]) + "\n", True))
                        else:
                            output.append(command_return[1][1] + "\n")
                    else:
                        output.append(" ".join([command_return[0], action.capitalize(), formatter.red('Failed')]))
            except Exception, e:
                logger.log("critical", e)
                return "Error"
        return "\n".join(output)

    else:
        for server in host_list:
            if action == "check":
                ssh_return = ssh.return_remote_command_std_output(server, "/etc/init.d/nvmesh%s status" % scope)
            elif action == "start":
                ssh_return = ssh.return_remote_command_std_output(server, "/etc/init.d/nvmesh%s start" % scope)
            elif action == "stop":
                ssh_return = ssh.return_remote_command_std_output(server, "/etc/init.d/nvmesh%s stop" % scope)
            elif action == "restart":
                ssh_return = ssh.return_remote_command_std_output(server, "/etc/init.d/nvmesh%s restart" % scope)
            if ssh_return[0] == 0:
                if details is True:
                    output.append(' '.join([formatter.bold(server), action.capitalize(), formatter.green('OK')]))
                    if prefix is True:
                        output.append(formatter.add_line_prefix(server, (ssh_return[1]), True))
                    else:
                        output.append((ssh_return[1] + "\n"))
                else:
                    output.append(" ".join([server, action.capitalize(), formatter.green('OK')]))
            else:
                if details is True:
                    output.append(' '.join([formatter.bold(server), action.capitalize(), formatter.red('Failed')]))
                    if prefix is True:
                        output.append(formatter.add_line_prefix(server, (ssh_return[1]), True))
                    else:
                        output.append((ssh_return[1] + "\n"))
                else:
                    output.append(" ".join([server, action.capitalize(), formatter.red('Failed')]))
        return "\n".join(output)


def manage_mcm(clients, action):
    ssh = SSHRemoteOperations()
    if clients is not None:
        client_list = clients
    else:
        client_list = get_client_list(False)
    for client in client_list:
        if action == "stop":
            ssh.execute_remote_command(client, "/opt/NVMesh/client-repo/management_cm/managementCMClient.py stop")
            print client, "\tStopped the MangaementCM services."
        elif action == "start":
            ssh.execute_remote_command(client, "/opt/NVMesh/client-repo/management_cm/managementCMClient.py start")
            print client, "\tStarted the MangaementCM services."
        elif action == "restart":
            ssh.execute_remote_command(client, "/opt/NVMesh/client-repo/management_cm/managementCMClient.py")
            print client, "\tRestarted the MangaementCM services."


def manage_cluster(details, action, prefix):
    if action == "check":
        print("Checking the NVMesh managers ...")
        NvmeshShell().poutput(manage_nvmesh_service('mgr', details, None, action, prefix, True, None))
        print("Checking the NVMesh targets ...")
        NvmeshShell().poutput(manage_nvmesh_service('target', details, None, action, prefix, True, None))
        print("Checking the NVMesh clients ...")
        NvmeshShell().poutput(manage_nvmesh_service('client', details, None, action, prefix, True, None))
    elif action == "start":
        print ("Starting the NVMesh managers ...")
        NvmeshShell().poutput(manage_nvmesh_service('mgr', details, None, action, prefix, True, None))
        time.sleep(3)
        print ("Starting the NVMesh targets ...")
        NvmeshShell().poutput(manage_nvmesh_service('target', details, None, action, prefix, True, None))
        print ("Starting the NVMesh clients ...")
        NvmeshShell().poutput(manage_nvmesh_service('client', details, None, action, prefix, True, None))
    elif action == "stop":
        print ("Stopping the NVMesh clients ...")
        NvmeshShell().poutput(manage_nvmesh_service('client', details, None, action, prefix, True, None))
        print ("Stopping the NVMesh targets ...")
        NvmeshShell().poutput(manage_nvmesh_service('target', details, None, action, prefix, True, True))
        print ("Stopping the NVMesh managers ...")
        NvmeshShell().poutput(manage_nvmesh_service('mgr', details, None, action, prefix, True, None))
    elif action == "restart":
        print ("Stopping the NVMesh clients ...")
        NvmeshShell().poutput(manage_nvmesh_service('client', details, None, 'stop', prefix, True, None))
        print ("Stopping the NVMesh targets ...")
        NvmeshShell().poutput(manage_nvmesh_service('target', details, None, 'stop', prefix, True, True))
        print ("Restarting the NVMesh managers ...")
        NvmeshShell().poutput(manage_nvmesh_service('mgr', details, None, 'restart', prefix, True, None))
        time.sleep(3)
        print ("Starting the NVMesh targets ...")
        NvmeshShell().poutput(manage_nvmesh_service('target', details, None, 'start', prefix, True, None))
        print ("Starting the NVMesh clients ...")
        NvmeshShell().poutput(manage_nvmesh_service('client', details, None, 'start', prefix, True, None))


def run_command(command, scope, prefix, parallel, server_list):
    host_list = []
    ssh = SSHRemoteOperations()
    command_line = " ".join(command)
    if server_list is not None:
        host_list = server_list
    else:
        if scope == 'cluster':
            host_list = get_target_list(short=True)
            host_list.extend(get_client_list(False))
            host_list.extend(mgmt.get_management_server_list())
        if scope == 'target':
            host_list = get_target_list(short=True)
        if scope == 'client':
            host_list = get_client_list(False)
        if scope == 'manager':
            host_list = mgmt.get_management_server_list()
        if scope == 'host':
            host_list = Hosts().manage_hosts('get', None, False)
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
                output.append(formatter.add_line_prefix(command_return[0], output_line, True))
            else:
                output.append(output_line)
        else:
            if len(command_return[1][1]) < 1:
                output_line = formatter.green("OK")
            else:
                output_line = command_return[1][1]
            if prefix is True:
                output.append(formatter.add_line_prefix(command_return[0], output_line, True))
            else:
                output.append(output_line)
    return "\n".join(output)


def run_parallel_ssh_command(argument):
    ssh = SSHRemoteOperations()
    try:
        output = ssh.return_remote_command_std_output(argument[0], argument[1])
        return argument[0], output
    except Exception, e:
        print e.message
        logger.log('critical', e)


def attach_detach_volumes(action, clients, volumes):
    try:
        process_pool = Pool(len(clients))
        parallel_execution_map = []
        command_return_list = []
        if action == 'attach':
            for client in clients:
                command_line = " ".join(['nvmesh_attach_volumes', " ".join(volumes)])
                parallel_execution_map.append([str(client), str(command_line)])
            command_return_list = process_pool.map(run_parallel_ssh_command, parallel_execution_map)
            process_pool.close()
        elif action == 'detach':
            for client in clients:
                command_line = " ".join(['nvmesh_detach_volumes', " ".join(volumes)])
                parallel_execution_map.append([str(client), str(command_line)])
            command_return_list = process_pool.map(run_parallel_ssh_command, parallel_execution_map)
            process_pool.close()
        output = []
        for command_return in command_return_list:
            output.append(formatter.add_line_prefix(command_return[0], command_return[1][1], False))
        return "\n".join(output)
    except Exception, e:
        print e.message
        logger.log('critical', e)


def manage_volume(action, name, capacity, description, disk_classes, server_classes, limit_by_nodes, limit_by_disks,
                  awareness, raid_level, stripe_width, number_of_mirrors, vpg, force):
    if get_api_ready() == 0:
        api_payload = {}
        payload = {}
        if action == "create":
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
                payload["domain"] = awareness[0]
            if raid_level is not None and vpg is None:
                payload["RAIDLevel"] = RAID_LEVELS[raid_level[0]]
                if raid_level[0] == "lvm":
                    pass
                elif raid_level[0] == "0":
                    payload["stripeSize"] = 32
                    payload["stripeWidth"] = int(stripe_width[0])
                elif raid_level[0] == "1":
                    payload["numberOfMirrors"] = int(number_of_mirrors[0]) if number_of_mirrors is not None else 1
                elif raid_level[0] == "10":
                    payload["stripeSize"] = 32
                    payload["stripeWidth"] = int(stripe_width[0])
                    payload["numberOfMirrors"] = int(number_of_mirrors[0]) if number_of_mirrors is not None else 1
            elif vpg is not None and raid_level is None:
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
                if force:
                    payload["force"] = True
                api_payload["remove"] = [payload]
                api_payload["create"] = []
                api_payload["edit"] = []
                api_return.append(json.loads(nvmesh.manage_volume(api_payload)))
            for item in api_return:
                if item['remove'][0]['success'] is True:
                    output.append(" ".join(["Volume", item['remove'][0]['id'], "successfully deleted.",
                                            formatter.green('OK')]))
                else:
                    output.append(" ".join([formatter.red('Failed'), "to delete", item['remove'][0]['id'], "-",
                                            item['remove'][0]['ex']]))
            return "\n".join(output)


def update_volume(volume, capacity, description, drives, targets, drive_classes, target_classes):
    if capacity:
        volume["capacity"] = "MAX" if str(capacity[0]).upper() == "MAX" else int(
            humanfriendly.parse_size(capacity[0], binary=True))
    if description:
        volume["description"] = " ".join(description)
    if drives:
        volume["limitByDisks"] = drives
    if targets:
        volume["limitByNodes"] = targets
    if target_classes:
        volume["serverClasses"] = target_classes
    if drive_classes:
        volume["diskClasses"] = drive_classes
    api_payload = dict()
    api_payload["remove"] = []
    api_payload["create"] = []
    api_payload["edit"] = [volume]
    api_return = json.loads(nvmesh.manage_volume(api_payload))
    if api_return["edit"][0]["success"] is True:
        output = " ".join(["Volume", volume["name"], "successfully updated.", formatter.green('OK')])
        return output
    else:
        output = " ".join([formatter.red('Failed'), "to update", volume["name"]])
        return output


def update_target_class(target_class, servers, description):
    if servers:
        target_class["targetNodes"] = servers
    if description:
        target_class["description"] = " ".join(description)
    api_payload = [target_class]
    api_return = json.loads(nvmesh.update_target_class(api_payload))
    print api_return
    if api_return[0]["success"] is True:
        output = " ".join(["target class", target_class["name"], "successfully updated.", formatter.green('OK')])
        return output
    else:
        output = " ".join([formatter.red('Failed'), "to update", target_class["name"], "-", api_return[0]["err"]])
        return output


def update_drive_class(drive_class, drives, description, file_path):
    if description:
        drive_class["description"] = description[0]
    if file_path:
        drive_class["disks"][0]["disks"] = parse_drive_args(open(file_path[0], 'r').readlines())
    if drives:
        drive_class["disks"][0]["disks"] = parse_drive_args(drives)
    api_payload = [drive_class]
    api_return = json.loads(nvmesh.update_drive_class(api_payload))
    if api_return[0]["success"] is True:
        output = " ".join(["Drive class", drive_class["_id"], "successfully updated.", formatter.green('OK')])
        return output
    else:
        output = " ".join([formatter.red('Failed'), "to update", drive_class["_id"]])
        return output


def manage_drive_class(action, class_list, drives, model, name, description, domains, file_path):
    if get_api_ready() == 0:
        api_return = []
        output = []
        payload = {}
        if action == "autocreate":
            model_list = get_drive_models(pretty=False)
            for model in model_list:
                drives = json.loads(nvmesh.get_disk_by_model(model[0]))
                drive_list = []
                for drive in drives:
                    drive_list.append(
                        {
                            "diskID": drive["disks"]["diskID"],
                            "node_id": drive["node_id"]
                        }
                    )
                payload["_id"] = re.sub("(?<=_)_|_(?=_)", "", model[0])
                payload["description"] = "automatically created"
                payload["disks"] = [{"model": model[0],
                                     "disks": drive_list}]
                api_payload = [payload]
                api_return.append([re.sub("(?<=_)_|_(?=_)", "", model[0]),
                                   json.dumps(nvmesh.manage_drive_class("save", api_payload))])
            for line in api_return:
                if "null" in line[1]:
                    output.append(" ".join(["Drive Class", line[0], "successfully created.", formatter.green('OK')]))
                else:
                    output.append(" ".join([formatter.red('Failed'), "\t", "Couldn't create Drive Class", line[0],
                                            " - ", "Check for duplicates."]))
            return "\n".join(output)
        elif action == "save":
            payload["_id"] = name[0]
            if description:
                payload["description"] = description
            if domains:
                payload["domains"] = parse_domain_args(domains)
            if file_path:
                payload["disks"] = [{"model": model[0],
                                     "disks": parse_drive_args(open(file_path[0], 'r').readlines())}]
            else:
                payload["disks"] = [{"model": model[0],
                                     "disks": parse_drive_args(drives)}]
            api_payload = [payload]
            api_return.append([name[0], json.dumps(nvmesh.manage_drive_class("save", api_payload))])
            for line in api_return:
                if "null" in line[1]:
                    output.append(" ".join(["Drive Class", name[0], "successfully created.", formatter.green('OK')]))
                else:
                    output.append(" ".join([formatter.red('Failed'), "\t", "Couldn't create Drive Class", name[0],
                                            " - ", "Check for duplicates."]))
            return "\n".join(output)

        elif action == "delete":
            for drive_class in class_list:
                payload = [{"_id": drive_class}]
                return_info = json.loads(nvmesh.manage_drive_class("delete", payload))

                if return_info[0]["success"] is True:
                    output.append(
                        " ".join(["Drive Class", drive_class, "successfully deleted.", formatter.green('OK')]))
                else:
                    output.append(" ".join([formatter.red('Failed'), "\t", "Couldn't delete Drive Class.", drive_class,
                                            " - ", return_info[0]["msg"]]))
            return "\n".join(output)


def manage_target_class(action, class_list, name, servers, description, domains):
    if get_api_ready() == 0:
        api_return = []
        output = []
        payload = {}
        if action == "autocreate":
            for target in get_target_list(short=False):
                payload["name"] = target.split(".")[0]
                payload["targetNodes"] = [target]
                payload["description"] = "automatically created"
                api_payload = [payload]
                api_return.append([target.split(".")[0], json.dumps(nvmesh.manage_target_class("save", api_payload))])
            for line in api_return:
                if "null" in line[1]:
                    output.append(" ".join(["Target Class", line[0], "successfully created.", formatter.green('OK')]))
                else:
                    output.append(" ".join([formatter.red('Failed'), "\t", "Couldn't create Target Class", line[0],
                                            " - ", "Check for duplicates."]))
            return "\n".join(output)
        elif action == "delete":
            for target_class in class_list:
                payload = [{"_id": target_class}]
                return_info = json.loads(nvmesh.manage_target_class("delete", payload))

                if return_info[0]["success"] is True:
                    output.append(
                        " ".join(["Target Class", target_class, "successfully deleted.", formatter.green('OK')]))
                else:
                    output.append(" ".join([formatter.red('Failed'), "\t", "Couldn't delete Target Class", target_class,
                                            " - ", return_info[0]["msg"]]))
            return "\n".join(output)
        elif action == "save":
            payload["name"] = name
            if description is not None:
                payload["description"] = description
            payload["targetNodes"] = servers
            if domains is not None:
                payload["domains"] = domains
            api_payload = [payload]
            api_return.append([name, json.dumps(nvmesh.manage_target_class("save", api_payload))])
            for line in api_return:
                if "null" in line[1]:
                    output.append(" ".join(["Target Class", line[0], "successfully created.", formatter.green('OK')]))
                else:
                    output.append(" ".join([formatter.red('Failed'), "\t", "Couldn't create Target Class", line[0],
                                            " - ", "Check for duplicates."]))
            return "\n".join(output)


def show_drives(details, targets, tsv):
    if get_api_ready() == 0:
        drive_list = []
        target_list = get_target_list(short=False)
        for target in target_list:
            if targets is not None and target.split('.')[0] not in targets:
                continue
            else:
                target_details = json.loads(nvmesh.get_server_by_id(target))
                for disk in target_details['disks']:
                    vendor = NVME_VENDORS.get(disk['Vendor'], disk['Vendor'])
                    status = u'\u2705' if disk["status"].lower() == "ok" else u'\u274C'
                    if details:
                        drive_list.append([vendor,
                                           disk['Model'],
                                           disk['diskID'],
                                           humanfriendly.format_size((disk['block_size'] * disk['blocks']),
                                                                     binary=True),
                                           status,
                                           humanfriendly.format_size(disk['block_size'], binary=True),
                                           " ".join([str(100 - int((disk['Available_Spare'].split("_")[0]))), "%"]),
                                           target,
                                           disk['Numa_Node'],
                                           disk['pci_root'],
                                           disk['Submission_Queues']])
                    else:
                        drive_list.append([vendor,
                                           re.sub("(?<=_)_|_(?=_)", "", disk['Model']),
                                           disk['diskID'],
                                           humanfriendly.format_size((disk['block_size'] * disk['blocks']),
                                                                     binary=True),
                                           status,
                                           target])
        if tsv:
            return formatter.print_tsv(drive_list)
        if details:
            return format_smart_table(sorted(drive_list),
                                      ['Vendor', 'Model', 'Drive ID', 'Size', 'Status', 'BS', 'Wear',
                                       'Target', 'Numa', 'PCI root', 'SubQ'])
        else:
            return format_smart_table(sorted(drive_list), ['Vendor', 'Model', 'Drive ID', 'Size', 'Status', 'Target'])


def show_drive_models(details):
    if not details:
        return format_smart_table(get_drive_models(pretty=True), ["Drive Model", "Drives"])
    else:
        return format_smart_table(get_drive_models(pretty=False), ["Drive Model", "Drives"])


def get_drive_models(pretty):
    if get_api_ready() == 0:
        model_list = []
        json_drive_models = json.loads(nvmesh.get_disk_models())
        for model in json_drive_models:
            if pretty:
                model_list.append([re.sub("(?<=_)_|_(?=_)", "", model["_id"]), model["available"]])
            else:
                model_list.append([model["_id"], model["available"]])
        return model_list


class NvmeshShell(Cmd):

    def __init__(self):
        Cmd.__init__(self, use_ipython=True)

    prompt = "\033[1;34mnvmesh #\033[0m "
    show_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    show_parser.add_argument('nvmesh_object', choices=['cluster', 'target', 'client', 'volume', 'drive', 'manager',
                                                       'sshuser', 'apiuser', 'vpg', 'driveclass', 'targetclass',
                                                       'host', 'log', 'drivemodel', 'version'],
                             help='Define/specify the scope or the NVMesh object you want to list or view.')
    show_parser.add_argument('-a', '--all', required=False, action='store_const', const=True, default=False,
                             help='Show all logs. Per default only alerts are shown.')
    show_parser.add_argument('-C', '--Class', nargs='+', required=False,
                             help='A single or a space separated list of NVMesh drives or target classes.')
    show_parser.add_argument('-d', '--detail', required=False, action='store_const', const=True,
                             help='Show more details.')
    show_parser.add_argument('-l', '--layout', required=False, action='store_const', const=True,
                             help='Show the volume layout details. To be used together with the "-d" switch.')
    show_parser.add_argument('-j', '--json', required=False, action='store_const', const=True,
                             help='Format output as JSON.')
    show_parser.add_argument('-s', '--server', nargs='+', required=False,
                             help='Space separated list or single server.')
    show_parser.add_argument('-S', '--short-name', required=False, action='store_const', const=True,
                             help='Show short hostnames.')
    show_parser.add_argument('-t', '--tsv', required=False, action='store_const', const=True,
                             help='Format output as tabulator separated values.')
    show_parser.add_argument('-v', '--volume', nargs='+', required=False,
                             help='View a single NVMesh volume or a list of volumes.')
    show_parser.add_argument('-p', '--vpg', nargs='+', required=False,
                             help='View a single or a list of NVMesh volume provisioning groups.')

    @with_argparser(show_parser)
    def do_show(self, args):
        """List and view specific Nvmesh objects and its properties.
The 'list sub-command allows output in a table, tabulator separated value or JSON format. E.g 'list targets' will list
all targets. In case you want to see the properties of only one or just a few you need to use the '-s' or '--server'
option to specify single or a list of servers/targets. E.g. 'list targets -s target1 target2'"""
        user.get_api_user()
        if args.nvmesh_object == 'target':
            self.poutput(show_targets(args.detail, args.tsv, args.json, args.server, args.short_name))
        elif args.nvmesh_object == 'client':
            self.poutput(show_clients(args.tsv, args.json, args.server, args.short_name))
        elif args.nvmesh_object == 'volume':
            self.poutput(show_volumes(args.detail, args.tsv, args.json, args.volume, args.short_name, args.layout))
        elif args.nvmesh_object == 'sshuser':
            self.poutput(user.get_ssh_user())
        elif args.nvmesh_object == 'apiuser':
            self.poutput(user.get_api_user())
        elif args.nvmesh_object == 'manager':
            self.poutput(show_manager())
        elif args.nvmesh_object == 'cluster':
            self.poutput(show_cluster(args.tsv, args.json))
        elif args.nvmesh_object == 'vpg':
            self.poutput(show_vpgs(args.tsv, args.json, args.vpg))
        elif args.nvmesh_object == 'driveclass':
            self.poutput(show_drive_classes(args.detail, args.tsv, args.json, args.Class))
        elif args.nvmesh_object == 'targetclass':
            self.poutput(show_target_classes(args.tsv, args.json, args.Class))
        elif args.nvmesh_object == 'host':
            self.poutput("\n".join(hosts.manage_hosts("get", None, False)))
        elif args.nvmesh_object == 'log':
            self.ppaged(show_logs(args.all))
        elif args.nvmesh_object == 'drive':
            self.poutput(show_drives(args.detail, args.server, args.tsv))
        elif args.nvmesh_object == 'drivemodel':
            self.poutput(show_drive_models(args.detail))
        elif args.nvmesh_object == 'version':
            self.poutput(version)

    add_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    add_parser.add_argument('nvmesh_object', choices=['host', 'volume', 'driveclass', 'targetclass'],
                            help='Add hosts to this shell environment or '
                                 'add/create new NVMesh volumes or drive classes.')
    add_parser.add_argument('-a', '--autocreate', required=False, action='store_const', const=True, default=False,
                            help='Create the drive classes automatically grouped by the available drive models.')
    add_parser.add_argument('-r', '--raid_level', nargs=1, required=False,
                            help='The RAID level of the volume. Options: lvm, 0, 1, 10')
    add_parser.add_argument('-v', '--vpg', nargs=1, required=False,
                            help='Optional - The volume provisioning group to use.')
    add_parser.add_argument('-o', '--domain', nargs=1, required=False,
                            help='Awareness domain information to use for new volume/s or a VPG.')
    add_parser.add_argument('-D', '--description', nargs=1, required=False,
                            help='Optional - Volume description')
    add_parser.add_argument('-l', '--limit-by-disk', nargs='+', required=False,
                            help='Optional - Limit volume allocation to specific drives.')
    add_parser.add_argument('-L', '--limit-by-target', nargs='+', required=False,
                            help='Optional - Limit volume allocation to specific target nodes.')
    group = add_parser.add_mutually_exclusive_group()
    group.add_argument('-m', '--drive', nargs='+', required=False,
                       help='Drive/media information. Needs to include the drive ID/serial and the target'
                            'node/server name in the format driveId:targetName'
                            'Example: -m "Example: 174019659DA4.1:test.lab"')
    group.add_argument('-f', '--file', nargs=1, required=False,
                       help='Path to the file containing the driveId:targetName information. '
                            'Needs to'
                            'Example: -f "/path/to/file". This argument is not allowed together with the -m '
                            'argument')
    add_parser.add_argument('-M', '--model', nargs=1, required=False,
                            help='Drive model information for the new drive class. '
                                 'Note: Must be the exactly the same model designator as when running the'
                                 '"show drivemodel -d" or "show drive -d" command!')
    add_parser.add_argument('-n', '--name', nargs=1, required=False,
                            help='Name of the volume, must be unique, will be the ID of the volume.')
    add_parser.add_argument('-N', '--number-of-mirrors', nargs=1, required=False,
                            help='Number of mirrors to use.')
    add_parser.add_argument('-O', '--classdomain', nargs='+', required=False,
                            help="Awareness domain/s information of the target or drive class. "
                                 "A domain has a scope and identifier component. "
                                 "You must provide both components for each domain to be used/created."
                                 "-O scope:Rack&identifier:A "
                                 "or in case you want to use more than one domain descriptor:"
                                 "-O scope:Rack&identifier:A scope:Datacenter&identifier:DRsite")
    add_parser.add_argument('-c', '--count', nargs=1, required=False,
                            help='Number of volumes to create and add. 100 Max.')
    add_parser.add_argument('-t', '--target-class', nargs='+', required=False,
                            help='Limit volume allocation to specific target classes.')
    add_parser.add_argument('-d', '--drive-class', nargs='+', required=False,
                            help='Limit volume allocation to specific drive classes.')
    add_parser.add_argument('-w', '--stripe-width', nargs=1, required=False,
                            help='Number of disks to use. Required for R0 and R10.')
    add_parser.add_argument('-s', '--server', nargs='+', required=False,
                            help='Specify a single server or a space separated list of servers.')
    add_parser.add_argument('-S', '--size', nargs=1, required=False,
                            help='Specify the size of the new volume. The volumes size value is base*2/binary. '
                                 'Example: -S 12GB or 12GiB will create a volume with a size of 12884901888 bytes.'
                                 'Some valid input formats samples: xGB, x GB, x gigabyte, x GiB or xG')

    @with_argparser(add_parser)
    def do_add(self, args):
        """The 'add' sub-command will let you add nvmesh objects to your cluster or nvmesh-shell runtime environment.
E.g. 'add hosts' will add host entries to your nvmesh-shell environment while 'add volume' will create and add a new
volume to the NVMesh cluster."""
        action = "add"
        if args.nvmesh_object == 'host':
            hosts.manage_hosts(action, args.server, False)
        elif args.nvmesh_object == 'driveclass':
            if args.autocreate:
                self.poutput(manage_drive_class("autocreate", None, None, None, None, None, None, None))
            else:
                if args.name is None:
                    print formatter.yellow(
                        "Drive class name missing! Use the -n argument to provide a name.")
                    return
                if not args.model:
                    print formatter.yellow(
                        "No drive model information specified. Use the -M argument to provide the drive model "
                        "information.")
                    return
                self.poutput(manage_drive_class("save", None, args.drive, args.model, args.name, args.description,
                                                args.classdomain, args.file))
        elif args.nvmesh_object == 'targetclass':
            if args.autocreate:
                self.poutput(manage_target_class("autocreate", None, None, None, None, None))
            else:
                if args.name is None:
                    print formatter.yellow(
                        "Target class name missing! Use the -n argument to provide a name.")
                    return
                if not args.server:
                    print formatter.yellow(
                        "No target servers specified. use the -s argument to provide a space separated list of targets"
                        "to be used. At least one target must be defined.")
                    return
                self.poutput(manage_target_class("save", None, args.name[0], args.server, args.description,
                                                 parse_domain_args(args.classdomain)))
        elif args.nvmesh_object == 'volume':
            if args.name is None:
                print(formatter.yellow(
                    "Volume name missing! Use the -n argument to provide a volume name"))
                return
            if args.size is None:
                print(formatter.yellow(
                    "Size/capacity information is missing! Use the -S argument to provide the volume size."))
                return
            if args.raid_level is None and args.vpg is None:
                print(formatter.yellow(
                    "Raid level information missing! Use the -r argument to set the raid level."))
                return
            if args.raid_level[0] != '1' and args.stripe_width is None:
                print(formatter.yellow(
                    "Stripe width information missing! Use the -w argument to set the stripe width."))
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
                                                   args.target_class, args.limit_by_target, args.limit_by_disk,
                                                   args.domain,
                                                   args.raid_level, args.stripe_width, args.number_of_mirrors,
                                                   args.vpg, None))
            else:
                self.poutput(manage_volume('create', args.name[0], args.size, args.description, args.drive_class,
                                           args.target_class, args.limit_by_target, args.limit_by_disk, args.domain,
                                           args.raid_level, args.stripe_width, args.number_of_mirrors, args.vpg, None))

    delete_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    delete_parser.add_argument('nvmesh_object', choices=['host', 'volume', 'driveclass', 'targetclass'],
                               help='Delete hosts, servers, drive classes and target classes.')
    delete_parser.add_argument('-s', '--server', nargs='+',
                               help='Specify a single server or a list of servers.')
    delete_parser.add_argument('-t', '--target-class', nargs='+',
                               help='Specify a single target class or a space separated list of target classes.')
    delete_parser.add_argument('-d', '--drive-class', nargs='+',
                               help='Specify a single drive class or a space separated list of drive classes.')
    delete_parser.add_argument('-v', '--volume', nargs='+',
                               help='Specify a single volume or a space separated list of volumes.')
    delete_parser.add_argument('-f', '--force', required=False, action='store_const', const=True, default=False,
                               help='Use this flag to forcefully delete the volume/s.')
    delete_parser.add_argument('-y', '--yes', required=False, action='store_const', const=True,
                               help='Automatically answer and skip operational warnings.')

    @with_argparser(delete_parser)
    def do_delete(self, args):
        """The 'delete' sub-command will let you delete nvmesh objects in your cluster or nvmesh-shell
runtime environment. E.g. 'delete hosts' will delete host entries in your nvmesh-shell environment and 'delete volume'
will delete NVMesh volumes in your NVMesh cluster."""
        action = "delete"
        if args.nvmesh_object == 'host':
            hosts.manage_hosts(action, args.server, False)
        elif args.nvmesh_object == 'targetclass':
            if args.target_class is None:
                print(formatter.yellow(
                    "Class information is missing! Use the -t/--target-class option to specify the class "
                    "or list of classes to be deleted."))
                return
            if args.target_class[0] == 'all':
                self.poutput(manage_target_class('delete', get_target_class_list(), None, None, None, None))
            else:
                self.poutput(manage_target_class('delete', args.target_class, None, None, None, None))
        elif args.nvmesh_object == 'driveclass':
            if args.drive_class is None:
                print(formatter.yellow(
                    "Class information is missing! Use the -d/--drive-class option to specify the class "
                    "or list of classes to be deleted."))
                return
            if args.drive_class[0] == 'all':
                self.poutput(manage_drive_class('delete', get_drive_class_list(), None, None, None, None, None, None))
            else:
                self.poutput(manage_drive_class('delete', args.drive_class, None, None, None, None, None, None))

        elif args.nvmesh_object == 'volume':
            if args.volume[0] == 'all':
                volume_list = get_volume_list()
            else:
                volume_list = args.volume
            if args.yes:
                self.poutput(manage_volume('remove', volume_list, None, None, None, None, None, None, None, None, None,
                                           None, None, args.force))
            else:
                if "y" in raw_input(WARNINGS['delete_volume']).lower():
                    self.poutput(manage_volume('remove', volume_list, None, None, None, None, None, None, None, None,
                                               None, None, None, args.force))
                else:
                    return

    attach_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    attach_parser.add_argument('-c', '--client', nargs='+', required=True,
                               help='Specify a single server or a space separated list of servers.')
    attach_parser.add_argument('-v', '--volume', nargs='+', required=True,
                               help='Specify a single volume or a space separated list of volumes.')

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
        self.poutput(attach_detach_volumes('attach', client_list, volume_list))

    detach_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    detach_parser.add_argument('-c', '--client', nargs='+', required=True,
                               help='Specify a single server or a space separated list of servers.')
    detach_parser.add_argument('-v', '--volume', nargs='+', required=True,
                               help='Specify a single volume or a space separated list of volumes.')
    detach_parser.add_argument('-y', '--yes', required=False, action='store_const', const=True,
                               help='Automatically answer and skip operational warnings.')

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
        self.poutput(attach_detach_volumes('detach', client_list, volume_list))

    check_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    check_parser.add_argument('nvmesh_object', choices=['client', 'target', 'manager', 'cluster'],
                              help='Specify where you want to check the NVMesh services status.')
    check_parser.add_argument('-d', '--detail', required=False, action='store_const', const=True,
                              help='Show detailed service information.')
    check_parser.add_argument('-p', '--prefix', required=False, action='store_const', const=True,
                              help='Adds the host name at the beginning of each line. This helps to identify the '
                                   'content when piping into a grep or similar')
    check_parser.add_argument('-P', '--parallel', required=False, action='store_const', const=True,
                              help='Check the hosts/servers in parallel.')
    check_parser.add_argument('-s', '--server', nargs='+', required=False,
                              help='Specify a single or a space separated list of managers, targets or clients.')

    @with_argparser(check_parser)
    def do_check(self, args):
        """The 'check' sub-command checks and let you list the status of the actual NVMesh services running in your
cluster. It is using SSH connectivity to the NVMesh managers, clients and targets to verify the service status. E.g.
'check targets' will check the NVMesh target services throughout the cluster."""
        user.get_ssh_user()
        user.get_api_user()
        action = "check"
        if args.nvmesh_object == 'target':
            self.poutput(manage_nvmesh_service('target', args.detail, args.server, action, args.prefix,
                                               args.parallel, False))
        elif args.nvmesh_object == 'client':
            self.poutput(manage_nvmesh_service('client', args.detail, args.server, action, args.prefix,
                                               args.parallel, False))
        elif args.nvmesh_object == 'manager':
            self.poutput(manage_nvmesh_service('mgr', args.detail, args.server, action, args.prefix,
                                               args.parallel, False))
        elif args.nvmesh_object == 'cluster':
            manage_cluster(args.detail, action, args.prefix)

    stop_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    stop_parser.add_argument('nvmesh_object', choices=['client', 'target', 'manager', 'cluster', 'mcm'],
                             help='Specify the NVMesh service type you want to top.')
    stop_parser.add_argument('-d', '--detail', required=False, action='store_const', const=True,
                             help='List and view the service details.')
    stop_parser.add_argument('-g', '--graceful', nargs=1, required=False, default="True", choices=['True', 'False'],
                             help="Graceful stop of all NVMesh targets in the cluster."
                                  " The default is set to 'True'")
    stop_parser.add_argument('-p', '--prefix', required=False, action='store_const', const=True,
                             help='Adds the host name at the beginning of each line. This helps to identify the '
                                  'content when piping into a grep or similar')
    stop_parser.add_argument('-P', '--parallel', required=False, action='store_const', const=True, default=True,
                             help='Stop the NVMesh services in parallel.')
    stop_parser.add_argument('-s', '--server', nargs='+', required=False,
                             help='Specify a single or a space separated list of managers, targets or clients.')
    stop_parser.add_argument('-y', '--yes', required=False, action='store_const', const=True,
                             help='Automatically answer and skip operational warnings.')

    @with_argparser(stop_parser)
    def do_stop(self, args):
        """The 'stop' sub-command will stop the selected NVMesh services on all managers, targets and clients.
Or it will stop the entire NVMesh cluster. It uses SSH connectivity to manage the NVMesh services.
E.g. 'stop clients' will stop all the NVMesh clients throughout the cluster."""
        user.get_ssh_user()
        user.get_api_user()
        action = "stop"
        if args.nvmesh_object == 'target':
            if args.yes:
                self.poutput(manage_nvmesh_service('target', args.detail, args.server, action, args.prefix,
                                                   args.parallel, (False if args.graceful[0] == "False" else True)))
            else:
                if "y" in raw_input(WARNINGS['stop_nvmesh_target']).lower():
                    self.poutput(manage_nvmesh_service('target', args.detail, args.server, action, args.prefix,
                                                       args.parallel, (False if args.graceful[0] == "False" else True)))
                else:
                    return
        elif args.nvmesh_object == 'client':
            if args.yes:
                self.poutput(manage_nvmesh_service('client', args.detail, args.server, action, args.prefix,
                                                   args.parallel, False))
            else:
                if "y" in raw_input(WARNINGS['stop_nvmesh_client']).lower():
                    self.poutput(manage_nvmesh_service('client', args.detail, args.server, action, args.prefix,
                                                       args.parallel, False))
                else:
                    return
        elif args.nvmesh_object == 'manager':
            if args.yes:
                self.poutput(manage_nvmesh_service('mgr', args.detail, args.server, action, args.prefix,
                                               args.parallel, False))
            else:
                if "y" in raw_input(WARNINGS['stop_nvmesh_manager']).lower():
                    self.poutput(manage_nvmesh_service('mgr', args.detail, args.server, action, args.prefix,
                                                       args.parallel, False))
                else:
                    return
        elif args.nvmesh_object == 'cluster':
            if args.yes:
                manage_cluster(args.detail, action, args.prefix)
            else:
                if "y" in raw_input(WARNINGS['stop_cluster']).lower():
                    manage_cluster(args.detail, action, args.prefix)
                else:
                    return
        elif args.nvmesh_object == 'mcm':
            manage_mcm(args.server, action)

    start_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    start_parser.add_argument('nvmesh_object', choices=['client', 'target', 'manager', 'cluster', 'mcm'],
                              help='Specify the NVMesh service type you want to start.')
    start_parser.add_argument('-d', '--detail', required=False, action='store_const', const=True,
                              help='List and view the service details.')
    start_parser.add_argument('-p', '--prefix', required=False, action='store_const', const=True,
                              help='Adds the host name at the beginning of each line. This helps to identify the '
                                   'content when piping into a grep or similar')
    start_parser.add_argument('-P', '--parallel', required=False, action='store_const', const=True, default=True,
                              help='Start the NVMesh services on the hosts/servers in parallel.')
    start_parser.add_argument('-s', '--server', nargs='+', required=False,
                              help='Specify a single or a space separated list of servers.')

    @with_argparser(start_parser)
    def do_start(self, args):
        """The 'start' sub-command will start the selected NVMesh services on all managers, targets and clients.
Or it will start the entire NVMesh cluster. It uses SSH connectivity to manage the NVMesh services.
E.g. 'start cluster' will start all the NVMesh services throughout the cluster."""
        user.get_ssh_user()
        user.get_api_user()
        action = "start"
        if args.nvmesh_object == 'target':
            self.poutput(manage_nvmesh_service('target', args.detail, args.server, action, args.prefix,
                                               args.parallel, False))
        elif args.nvmesh_object == 'client':
            self.poutput(manage_nvmesh_service('client', args.detail, args.server, action, args.prefix,
                                               args.parallel, False))
        elif args.nvmesh_object == 'manager':
            self.poutput(manage_nvmesh_service('mgr', args.detail, args.server, action, args.prefix,
                                               args.parallel, False))
        elif args.nvmesh_object == 'cluster':
            manage_cluster(args.detail, action, args.prefix)
        elif args.nvmesh_object == 'mcm':
            manage_mcm(args.server, action)

    restart_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    restart_parser.add_argument('nvmesh_object', choices=['client', 'target', 'manager', 'cluster', 'mcm'],
                                help='Specify the NVMesh service which you want to restart.')
    restart_parser.add_argument('-d', '--detail', required=False, action='store_const', const=True,
                                help='List and view the service details.')
    restart_parser.add_argument('-g', '--graceful', nargs=1, required=False, default="True", choices=['True', 'False'],
                                help='Restart with a graceful stop of the targets in the cluster.'
                                     'The default is set to True')
    restart_parser.add_argument('-p', '--prefix', required=False, action='store_const', const=True,
                                help='Adds the host name at the beginning of each line. This helps to identify the '
                                     'content when piping into a grep or similar')
    restart_parser.add_argument('-P', '--parallel', required=False, action='store_const', const=True, default=True,
                                help='Restart the NVMesh services on the hosts/servers in parallel.')
    restart_parser.add_argument('-s', '--server', nargs='+', required=False,
                                help='Specify a single or a space separated list of servers.')
    restart_parser.add_argument('-y', '--yes', required=False, action='store_const', const=True,
                                help='Automatically answer and skip operational warnings.')

    @with_argparser(restart_parser)
    def do_restart(self, args):
        """The 'restart' sub-command will restart the selected NVMesh services on all managers, targets and clients.
Or it will restart the entire NVMesh cluster. It uses SSH connectivity to manage the NVMesh services.
E.g. 'restart managers' will restart the NVMesh management service."""
        user.get_ssh_user()
        user.get_api_user()
        action = 'restart'
        if args.nvmesh_object == 'target':
            self.poutput(manage_nvmesh_service('target', args.detail, args.server, action, args.prefix,
                                               args.parallel, (False if args.graceful[0] == "False" else True)))
        elif args.nvmesh_object == 'client':
            self.poutput(manage_nvmesh_service('client', args.detail, args.server, action, args.prefix,
                                               args.parallel, False))
        elif args.nvmesh_object == 'manager':
            self.poutput(manage_nvmesh_service('mgr', args.detail, args.server, action, args.prefix,
                                               args.parallel, False))
        elif args.nvmesh_object == 'mcm':
            manage_mcm(args.server, action)
        elif args.nvmesh_object == 'cluster':
            manage_cluster(args.detail, action, args.prefix)

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
                ManagementServer().save_management_server(raw_input(
                    "Provide a space separated list, min. one, of the NVMesh manager server names: ").split(" "))
            else:
                mgmt.server = args.server[0]
            if args.persistent is True:
                mgmt.save_management_server(args.server)
                mgmt.server = args.server[0]

    def do_license(self):
        """Shows the licensing details, terms and conditions. """
        package_dir = os.path.dirname(os.path.abspath(__file__))
        license_file_path = os.path.join(package_dir, 'LICENSE.txt')
        self.ppaged(open(license_file_path, 'r').read())

    runcmd_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    runcmd_parser.add_argument('scope', choices=['client', 'target', 'manager', 'cluster', 'host'],
                               help='Specify the scope where you want to run the command.')
    runcmd_parser.add_argument('-c', '--command', nargs='+', required=True,
                               help='The command you want to run on the servers. Use quotes if the command needs to run'
                                    ' with flags by itself, like: runcmd cluster -c "uname -a"')
    runcmd_parser.add_argument('-p', '--prefix', required=False, action='store_const', const=True,
                               help='Adds the host name at the beginning of each line. This helps to identify the '
                                    'content when piping into a grep or similar tasks.')
    runcmd_parser.add_argument('-P', '--parallel', required=False, action='store_const', const=True,
                               help='Runs the remote command on the remote hosts in parallel.')
    runcmd_parser.add_argument('-s', '--server', nargs='+', required=False,
                               help='Specify list of servers and or hosts.')

    @with_argparser(runcmd_parser)
    def do_runcmd(self, args):
        """Run a remote shell command across the whole NVMesh cluster, or just the targets, clients, managers or a list
        of selected servers and hosts.
Excample: runcmd managers -c systemctl status mongod"""
        user.get_ssh_user()
        user.get_api_user()
        self.poutput(run_command(args.command, args.scope, args.prefix, args.parallel, args.server))

    testssh_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    testssh_parser.add_argument('-s', '--server', nargs='+', required=False,
                                help='Specify a server or a list of servers and/or hosts.')

    @with_argparser(testssh_parser)
    def do_testssh(self, args):
        """Test the SSH connectivity to all, a list of, or individual servers and hosts.
Excample: testssh -s servername"""
        ssh = SSHRemoteOperations()
        user.get_ssh_user()
        ssh.test_ssh_connection(args.server)

    update_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    update_parser.add_argument('object', choices=['volume', 'driveclass', 'targetclass'],
                               help='Specify the NVMesh object to be updated.')
    update_parser.add_argument('-n', '--name', nargs=1, required=True,
                               help='The name of the object to be updated.')
    update_parser.add_argument('-S', '--size', nargs='+', required=False,
                               help='The new/updated size/capacity of the volume.\n '
                                    'The volumes size value is base*2/binary. \n'
                                    'Example: -s 12GB or 12GiB will size the volume with a size of 12884901888 bytes.\n'
                                    'Some valid input formats samples: xGB, x GB, x gigabyte, x GiB or xG')
    update_parser.add_argument('-D', '--description', required=False, nargs='+',
                               help='The new/updated name of the NVMesh object.')
    update_parser.add_argument('-s', '--server', nargs='+', required=False, help='Specify a single server or a space '
                                                                                 'separated list of servers.')
    group = update_parser.add_mutually_exclusive_group()
    group.add_argument('-m', '--drive', nargs='+', required=False,
                       help='Drive/media information. Needs to include the drive ID/serial and the target'
                            'node/server name in the format driveId:targetName'
                            'Example: -m "Example: 174019659DA4.1:test.lab"')
    group.add_argument('-f', '--file', nargs=1, required=False,
                       help='Path to the file containing the driveId:targetName information. '
                            'Needs to'
                            'Example: -f "/path/to/file". This argument is not allowed together with the -m '
                            'argument')
    update_parser.add_argument('-l', '--limit-by-disk', nargs='+', required=False,
                               help='Optional - Limit volume allocation to specific drives.')
    update_parser.add_argument('-L', '--limit-by-target', nargs='+', required=False,
                               help='Optional - Limit volume allocation to specific target nodes.')
    update_parser.add_argument('-t', '--target-class', nargs='+', required=False,
                               help='Optional - Limit volume allocation to specific target classes.')
    update_parser.add_argument('-d', '--drive-class', nargs='+', required=False,
                               help='Optional - Limit volume allocation to specific drive classes.')

    @with_argparser(update_parser)
    def do_update(self, args):
        """Update and edit an existing NVMesh volume, driveclass or targetclass."""
        if get_api_ready() == 0:
            if args.object == 'volume':
                volume = json.loads(nvmesh.get_volume(args.name[0]))
                if len(volume) == 0:
                    print(formatter.yellow("%s is not a valid volume name. A volume with this name doesn't exist."
                                           % args.name[0]))
                    return
                else:
                    self.poutput(update_volume(volume[0], args.size, args.description, args.limit_by_disk,
                                               args.limit_by_target, args.drive_class, args.target_class))
            elif args.object == 'targetclass':
                target_class = json.loads(nvmesh.get_target_class(args.name[0]))
                if len(target_class) == 0:
                    print(formatter.yellow("%s is not a valid target class name. "
                                           "A target class with this name doesn't exist."
                                           % args.name[0]))
                    return
                else:
                    self.poutput(update_target_class(target_class[0], args.server, args.description))
            elif args.object == 'driveclass':
                drive_class = json.loads(nvmesh.get_drive_class(args.name[0]))
                if len(drive_class) == 0:
                    print(formatter.yellow(
                        "%s is not a valid drive class name. A drive class with this name doesn't exist."
                        % args.name[0]))
                    return
                else:
                    self.poutput(update_drive_class(drive_class[0], args.drive, args.description, args.file))


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

Starting the NVMesh shell version %s ...''' % version)


if __name__ == '__main__':
    start_shell()
