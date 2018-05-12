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
import csv
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


class OutputFormatter:
    def __init__(self):
        self.text = None
        self.host = None

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
    def print_csv(content):
        writer = csv.writer(sys.stdout, delimiter='\t', lineterminator='\n')
        for line in content:
            writer.writerow(line)
        return

    @staticmethod
    def print_json(content):
        print(json.dumps(content, indent=2))
        return


class Hosts:
    def __init__(self):
        self.host_list = []
        self.host_file = os.path.abspath('nvmesh_hosts')
        self.test_host_connection_test_result = None
        self.formatter = OutputFormatter()
        self.host_delete_list = []


    def read_host_list(self):
        self.host_list = open(self.host_file, 'r').readlines()
        return self.host_list

    def verify_host_list(self):
        if os.path.isfile(self.host_file):
            print ("Found the following hosts/servers configured:")
            self.print_hosts()
        else:
            print(self.formatter.print_yellow('No hosts/servers defined!'))

    def print_hosts(self):
        if os.path.isfile(self.host_file):
            for host in self.read_host_list():
                print host.strip()
        else:
            print "Server list file doesnt exist"
        return

    def write_host_list(self, hosts_string):
        for host in (str(hosts_string).split()):
            open(self.host_file, 'a').write(host.strip() + "\n")

    def delete_host_from_list(self, host_delete_list):
        host_list = self.read_host_list()
        for host in host_delete_list:
            host_list.remove(host)
        os.remove("host.list")
        for host in host_list:
            open(self.host_file, 'a').write(host)


class ManagementServer:
    def __init__(self):
        self.server = None
        self.server_list = []
        self.server_file = os.path.abspath('nvmesh_manager')

    def get_management_server(self):
        if os.path.isfile(self.server_file):
            self.server = open(self.server_file, 'r').read()
            return self.server
        else:
            formatter.print_yellow("No API management server defined yet! Run 'define manager' first!")
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
            return
        else:
            self.SSH_user_name = self.secrets[0]
            self.SSH_password = base64.b64decode(self.secrets[1])

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
            return
        else:
            self.API_user_name = self.secrets[0]
            self.API_password = base64.b64decode(self.secrets[1])


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
            elif self.remote_command_return == 255:
                return self.remote_command_return[0], remote_command + " shows no data as there is no IB transport layer. Looks like Ethernet " \
                                        "connectivity."
            elif self.remote_command_return[0] != 0:
                return self.remote_command_return[0], self.remote_command_return[1]
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
    nvmesh.api_server = mgmt.server
    nvmesh.login()
    return


def list_cluster(csv_format, json_format):
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
        cluster_volumes.append(' '.join([`count`, volume]))
    cluster_list.append([total_server, offline_server, total_clients, offline_clients,
                            '; '.join(cluster_volumes),
                            humanfriendly.format_size(capacity_json['totalCapacityInBytes'], binary=True),
                            humanfriendly.format_size(capacity_json['availableSpaceInBytes'], binary=True)])
    if csv_format is True:
        formatter.print_csv(cluster_list)
        return
    if json_format is True:
        formatter.print_json(cluster_list)
        return
    print(format_smart_table(cluster_list,
                             ['Total Servers', 'Offline Servers', 'Total Clients', 'Offline Clients',
                              'Volumes', 'Total Capacity', 'Available Space']))


def list_targets(details, csv_format, json_format, server):
    get_api_ready()
    target_json = json.loads(nvmesh.get_servers())
    target_list = []
    for target in target_json:
        if server is not None and target['node_id'].split('.')[0] not in server:
            continue
        else:
            target_disk_list = []
            target_nic_list = []
            for disk in target['disks']:
                target_disk_list.append(disk['diskID'])
            for nic in target['nics']:
                target_nic_list.append(nic['nicID'])
                if details is True:
                    target_list.append([target['node_id'], target['health'], target['version'],
                                        '; '.join(target_disk_list),
                                    ' '.join(target_nic_list)])
                else:
                    target_list.append([target['node_id'], target['health'], target['version']])
    if details is True:
        if csv_format is True:
            formatter.print_csv(target_list)
            return
        if json_format is True:
            formatter.print_json(target_list)
        else:
            print(format_smart_table(target_list,
                                     ['Target Name', 'Target Health', 'NVMesh Version', 'Target Disks',
                                      'Target NICs']))
    else:
        if csv_format is True:
            formatter.print_csv(target_list)
            return
        else:
            print(format_smart_table(target_list, ['Target Name', 'Target Health', 'NVMesh Version']))
            return


def get_target_list():
    get_api_ready()
    target_json = json.loads(nvmesh.get_servers())
    target_list = []
    for target in target_json:
        target_list.append(target['node_id'])
    return target_list


def get_client_list():
    get_api_ready()
    clients_json = json.loads(nvmesh.get_clients())
    client_list = []
    for client in clients_json:
            client_list.append(client['client_id'])
    return client_list


def list_clients(csv_format, server):
    get_api_ready()
    clients_json = json.loads(nvmesh.get_clients())
    client_list = []
    volume_list = []
    for client in clients_json:
        if server is not None and client['client_id'].split('.')[0] not in server:
            continue
        else:
            for volume in client['block_devices']:
                volume_list.append(volume['name'])
            client_list.append([client['client_id'], client['health'], client['version'], '; '.join(volume_list)])
    if csv_format is True:
        formatter.print_csv(client_list)
    else:
        print(format_smart_table(client_list, ['Client Name', 'Client Health', 'Client Version', 'Client Volumes']))


def list_volumes(details, csv_format, volumes):
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
                        target_list.append(segment['node_id'])
                        target_disk_list.append(segment['diskID'])
        if details is True:
            volumes_list.append([volume['name'], volume['health'], volume['status'], volume['RAIDLevel'],
                                 humanfriendly.format_size(volume['capacity'], binary=True), volume['stripeWidth'],
                                 humanfriendly.format_size((remaining_dirty_bits * 4096), binary=True),
                                 '; '.join(set(target_list)), '; '.join(set(target_disk_list))])
        else:
            volumes_list.append([volume['name'], volume['health'], volume['status'], volume['RAIDLevel'],
                                 humanfriendly.format_size(volume['capacity'], binary=True), volume['stripeWidth'],
                                 humanfriendly.format_size((remaining_dirty_bits * 4096), binary=True)])
    if details is True:
        if csv_format is True:
            formatter.print_csv(volumes_list)
        else:
            print(format_smart_table(volumes_list,
                             ['Volume Name', 'Volume Health', 'Volume Status', 'Volume Type', 'Volume Size',
                              'Stripe Width', 'Dirty Bits', 'Target Names', 'Target Disks']))
    else:
        if csv_format is True:
            formatter.print_csv(volumes_list)
        else:
            print(format_smart_table(volumes_list,
                                 ['Volume Name', 'Volume Health', 'Volume Status', 'Volume Type', 'Volume Size',
                                  'Stripe Width', 'Dirty Bits']))


def list_vpgs(csv_format, json_format, vpgs):
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

        vpgs_list.append([vpg['name'], vpg['RAIDLevel'], vpg_stripe_width, humanfriendly.format_size(vpg['capacity'], binary=True),
                          '; '.join(disk_classes_list), '; '.join(server_classes_list)])
    if csv_format is True:
        formatter.print_csv(vpgs_list)
        return
    elif json_format is True:
        formatter.print_json(vpgs_list)
        return
    else:
        print(format_smart_table(vpgs_list, ['VPG Name', 'RAID Level', 'Stripe Width', 'Reserved Capacity',
                                         'Disk Classes', 'Target Classes']))


def list_drive_classes(details, csv_format, json_format, classes):
    get_api_ready()
    drive_classes_json = json.loads(nvmesh.get_disk_classes())
    drive_class_list = []
    drive_model_list = []
    drive_target_list = []
    for drive_class in drive_classes_json:
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
        formatter.print_csv(drive_class_list)
    elif json_format is True:
        formatter.print_json(drive_class_list)
    else:
        print(format_smart_table(drive_class_list, ['Drive Class', 'Drive Models', 'Drive Details']))


def list_target_classes(csv_format, json_format, classes):
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
        formatter.print_csv(target_classes_list)
    elif json_format is True:
        formatter.print_json(target_classes_list)
    else:
        print(format_smart_table(target_classes_list, ['Target Class', 'Description', 'Target Nodes']))


def check_targets(details, targets):
    print formatter.bold_underline('Checking the NVMesh targets ...')
    for target in get_target_list():
        if targets is not None and target.split('.')[0] not in targets:
            continue
        else:
            ssh = SSHRemoteOperations()
            ssh_return = ssh.return_remote_command_std_output(target, CMD_STATUS_NVMESH_TARGET)
            if ssh_return[0] == 0:
                if details is True:
                    print formatter.bold(target), formatter.green('OK')
                    print ssh_return[1][:ssh_return[1].rfind('\n')], "\n"
                else:
                    print target, formatter.green('OK')
            else:
                if details is True:
                    print formatter.bold(target), formatter.red('Failed')
                    print ssh_return[1][:ssh_return[1].rfind('\n')], "\n"
                else:
                    print target, formatter.red('Failed')


def count_active_targets():
    active_targets = 0
    for target in get_target_list():
        ssh = SSHRemoteOperations()
        ssh_return = ssh.return_remote_command_std_output(target, CMD_STATUS_NVMESH_TARGET)
        if ssh_return[0] == 0:
            active_targets += 1
    return active_targets


def stop_targets(details, targets, force):
    if force is True:
        print formatter.bold_underline('Stopping the NVMesh targets forcefully ...')
        for target in get_target_list():
            if targets is not None and target.split('.')[0] not in targets:
                continue
            else:
                ssh = SSHRemoteOperations()
                ssh_return = ssh.return_remote_command_std_output(target, CMD_STOP_NVMESH_TARGET)
                if ssh_return[0] == 0:
                    if details is True:
                        print formatter.bold(target), formatter.green('OK')
                        print ssh_return[1][:ssh_return[1].rfind('\n')], "\n"
                    else:
                        print target, formatter.green('OK')
                else:
                    if details is True:
                        print formatter.bold(target), formatter.red('Failed')
                        print ssh_return[1][:ssh_return[1].rfind('\n')], "\n"
                    else:
                        print target, formatter.red('Failed')
    else:
        print formatter.bold_underline('Stopping the NVMesh targets ...')
        get_api_ready()
        nvmesh.target_cluster_shutdown()
        print 'Please wait ...'
        while count_active_targets() != 0:
            time.sleep(5)
        print 'Nvmesh target cluster shutdown', formatter.green('OK')


def start_targets(details, targets):
    print formatter.bold_underline('Starting the NVMesh targets ...')
    for target in get_target_list():
        if targets is not None and target.split('.')[0] not in targets:
            continue
        else:
            ssh = SSHRemoteOperations()
            ssh_return = ssh.return_remote_command_std_output(target, CMD_START_NVMESH_TARGET)
            if ssh_return[0] == 0:
                if details is True:
                    print formatter.bold(target), formatter.green('OK')
                    print ssh_return[1][:ssh_return[1].rfind('\n')], "\n"
                else:
                    print target, formatter.green('OK')
            else:
                if details is True:
                    print formatter.bold(target), formatter.red('Failed')
                    print ssh_return[1][:ssh_return[1].rfind('\n')], "\n"
                else:
                    print target, formatter.red('Failed')


def restart_targets(details, targets):
    print formatter.bold_underline('Restarting the NVMesh targets ...')
    for target in get_target_list():
        if targets is not None and target.split('.')[0] not in targets:
            continue
        else:
            ssh = SSHRemoteOperations()
            ssh_return = ssh.return_remote_command_std_output(target, CMD_RESTART_NVMESH_TARGET)
            if ssh_return[0] == 0:
                if details is True:
                    print formatter.bold(target), formatter.green('OK')
                    print ssh_return[1][:ssh_return[1].rfind('\n')], "\n"
                else:
                    print target, formatter.green('OK')
            else:
                if details is True:
                    print formatter.bold(target), formatter.red('Failed')
                    print ssh_return[1][:ssh_return[1].rfind('\n')], "\n"
                else:
                    print target, formatter.red('Failed')


def check_clients(details, clients):
    print formatter.bold_underline('Checking the NVMesh clients ...')
    for client in get_client_list():
        if clients is not None and client.split('.')[0] not in clients:
            continue
        else:
            ssh = SSHRemoteOperations()
            ssh_return = ssh.return_remote_command_std_output(client, CMD_STATUS_NVMESH_CLIENT)
            if ssh_return[0] == 0:
                if details is True:
                    print formatter.bold(client), formatter.green('OK')
                    print ssh_return[1][:ssh_return[1].rfind('\n')], "\n"
                else:
                    print client, formatter.green('OK')
            else:
                if details is True:
                    print formatter.bold(client), formatter.red('Failed')
                    print ssh_return[1][:ssh_return[1].rfind('\n')], "\n"
                else:
                    print client, formatter.red('Failed')


def stop_clients(details, clients):
    print formatter.bold_underline('Stopping the NVMesh clients ...')
    for client in get_client_list():
        if clients is not None and client.split('.')[0] not in clients:
            continue
        else:
            ssh = SSHRemoteOperations()
            ssh_return = ssh.return_remote_command_std_output(client, CMD_STOP_NVMESH_CLIENT)
            if ssh_return[0] == 0:
                if details is True:
                    print formatter.bold(client), formatter.green('OK')
                    print ssh_return[1][:ssh_return[1].rfind('\n')], "\n"
                else:
                    print client, formatter.green('OK')
            else:
                if details is True:
                    print formatter.bold(client), formatter.red('Failed')
                    print ssh_return[1][:ssh_return[1].rfind('\n')], "\n"
                else:
                    print client, formatter.red('Failed')


def stop_mcm(clients):
    print formatter.bold_underline('Stopping the NVMesh ManagementCM services ...')
    for client in get_client_list():
        if clients is not None and client.split('.')[0] not in clients:
            continue
        else:
            ssh = SSHRemoteOperations()
            ssh.execute_remote_command(client, CMD_STOP_NVMESH_MCM)
            print client, "\tStopped the MangaementCM services."


def start_mcm(clients):
    print formatter.bold_underline('Starting the NVMesh ManagementCM services ...')
    for client in get_client_list():
        if clients is not None and client.split('.')[0] not in clients:
            continue
        else:
            ssh = SSHRemoteOperations()
            ssh.execute_remote_command(client, CMD_START_NVMESH_MCM)
            print client, "\tStarted the MangaementCM service"


def restart_mcm(clients):
    print formatter.bold_underline('Restarting the NVMesh ManagementCM services ...')
    for client in get_client_list():
        if clients is not None and client.split('.')[0] not in clients:
            continue
        else:
            ssh = SSHRemoteOperations()
            ssh.execute_remote_command(client, CMD_RESTART_NVMESH_MCM)
            print client, "\tRestarted the MangaementCM"


def start_clients(details, clients):
    print formatter.bold_underline('Starting the NVMesh clients ...')
    for client in get_client_list():
        if clients is not None and client.split('.')[0] not in clients:
            continue
        else:
            ssh = SSHRemoteOperations()
            ssh_return = ssh.return_remote_command_std_output(client, CMD_START_NVMESH_CLIENT)
            if ssh_return[0] == 0:
                if details is True:
                    print formatter.bold(client), formatter.green('OK')
                    print ssh_return[1][:ssh_return[1].rfind('\n')], "\n"
                else:
                    print client, formatter.green('OK')
            else:
                if details is True:
                    print formatter.bold(client), formatter.red('Failed')
                    print ssh_return[1][:ssh_return[1].rfind('\n')], "\n"
                else:
                    print client, formatter.red('Failed')


def restart_clients(details, clients):
    print formatter.bold_underline('Restarting the NVMesh clients ...')
    for client in get_client_list():
        if clients is not None and client.split('.')[0] not in clients:
            continue
        else:
            ssh = SSHRemoteOperations()
            ssh_return = ssh.return_remote_command_std_output(client, CMD_RESTART_NVMESH_CLIENT)
            if ssh_return[0] == 0:
                if details is True:
                    print formatter.bold(client), formatter.green('OK')
                    print ssh_return[1][:ssh_return[1].rfind('\n')], "\n"
                else:
                    print client, formatter.green('OK')
            else:
                if details is True:
                    print formatter.bold(client), formatter.red('Failed')
                    print ssh_return[1][:ssh_return[1].rfind('\n')], "\n"
                else:
                    print client, formatter.red('Failed')


def check_managers(details, managers):
    print formatter.bold_underline('Checking the NVMesh managers ...')
    for manager in mgmt.get_management_server_list():
        if managers is not None and manager not in managers:
            continue
        else:
            ssh = SSHRemoteOperations()
            ssh_return = ssh.return_remote_command_std_output(manager, CMD_STATUS_NVMESH_MANAGER)
            if ssh_return[0] == 0:
                if details is True:
                    print "\033[1m" + manager + "\033[0m", formatter.green('OK')
                    print ssh_return[1][:ssh_return[1].rfind('\n')], "\n"
                else:
                    print "\033[1m" + manager + "\033[0m", formatter.green('OK')
            else:
                if details is True:
                    print "\033[1m" + manager + "\033[0m", formatter.red('Failed')
                    print ssh_return[1][:ssh_return[1].rfind('\n')], "\n"
                else:
                    print "\033[1m" + manager + "\033[0m", formatter.red('Failed')


def stop_managers(details, managers):
    print formatter.bold_underline('Stopping the NVMesh managers ...')
    for manager in mgmt.get_management_server_list():
        if managers is not None and manager not in managers:
            continue
        else:
            ssh = SSHRemoteOperations()
            ssh_return = ssh.return_remote_command_std_output(manager, CMD_STOP_NVMESH_MANAGER)
            if ssh_return[0] == 0:
                if details is True:
                    print "\033[1m" + manager + "\033[0m", formatter.green('OK')
                    print ssh_return[1][:ssh_return[1].rfind('\n')], "\n"
                else:
                    print "\033[1m" + manager + "\033[0m", formatter.green('OK')
            else:
                if details is True:
                    print "\033[1m" + manager + "\033[0m", formatter.red('Failed')
                    print ssh_return[1][:ssh_return[1].rfind('\n')], "\n"
                else:
                    print "\033[1m" + manager + "\033[0m", formatter.red('Failed')


def start_managers(details, managers):
    print formatter.bold_underline('Starting the NVMesh managers ...')
    for manager in mgmt.get_management_server_list():
        if managers is not None and manager not in managers:
            continue
        else:
            ssh = SSHRemoteOperations()
            ssh_return = ssh.return_remote_command_std_output(manager, CMD_START_NVMESH_MANAGER)
            if ssh_return[0] == 0:
                if details is True:
                    print "\033[1m" + manager + "\033[0m", formatter.green('OK')
                    print ssh_return[1][:ssh_return[1].rfind('\n')], "\n"
                else:
                    print "\033[1m" + manager + "\033[0m", formatter.green('OK')
            else:
                if details is True:
                    print "\033[1m" + manager + "\033[0m", formatter.red('Failed')
                    print ssh_return[1][:ssh_return[1].rfind('\n')], "\n"
                else:
                    print "\033[1m" + manager + "\033[0m", formatter.red('Failed')


def restart_managers(details, managers):
    print formatter.bold_underline('Restarting the NVMesh managers ...')
    for manager in mgmt.get_management_server_list():
        if managers is not None and manager not in managers:
            continue
        else:
            ssh = SSHRemoteOperations()
            ssh_return = ssh.return_remote_command_std_output(manager, CMD_RESTART_NVMESH_MANAGER)
            if ssh_return[0] == 0:
                if details is True:
                    print "\033[1m" + manager + "\033[0m", formatter.green('OK')
                    print ssh_return[1][:ssh_return[1].rfind('\n')], "\n"
                else:
                    print "\033[1m" + manager + "\033[0m", formatter.green('OK')
            else:
                if details is True:
                    print "\033[1m" + manager + "\033[0m", formatter.red('Failed')
                    print ssh_return[1][:ssh_return[1].rfind('\n')], "\n"
                else:
                    print "\033[1m" + manager + "\033[0m", formatter.red('Failed')


def check_cluster():
    check_managers(None, None)
    check_targets(None, None)
    check_clients(None, None)


def start_cluster(details):
    start_managers(details, None)
    time.sleep(3)
    start_targets(details, None)
    start_clients(details, None)


def stop_cluster(details, force):
    stop_clients(details, None)
    stop_targets(details, None, force)
    stop_managers(details, None)


class NvmeshShell(Cmd):
    prompt = "\033[1;34mnvmesh #\033[0m "
    list_parser = argparse.ArgumentParser()
    list_parser.add_argument('nvmesh_object', choices=['cluster', 'targets', 'clients', 'volumes', 'manager',
                                                       'sshuser', 'apiuser','vpgs', 'driveclasses', 'targetclasses'],
                             default='cluster',
                             nargs='?',
                             help='Define/specify the NVMesh object you want to list or view.')
    list_parser.add_argument('-a', '--all', required=False, default=True, action='store_const', const=True,
                             help='List and view all NVMesh objects throughout the cluster')
    list_parser.add_argument('-c', '--classes', nargs='+', required=False,
                             help='View a single or a list of NVMesh drive or target classes.')
    list_parser.add_argument('-d', '--details', required=False, action='store_const', const=True,
                             help='Show more details.')
    list_parser.add_argument('-j', '--json', required=False, action='store_const', const=True,
                             help='Format output as JSON.')
    list_parser.add_argument('-s', '--servers', nargs='+', required=False,
                             help='List or view NVMesh objects of a single server or a list of servers.')
    list_parser.add_argument('-t', '--tsv', required=False, action='store_const', const=True,
                             help='Format output as tabulator separated values.')
    list_parser.add_argument('-v', '--volumes', nargs='+', required=False,
                             help='View a single NVMesh volume or a list of volumes.')
    list_parser.add_argument('-p', '--vpgs', nargs='+', required=False,
                             help='View a single or a list of NVMesh volume provisioning groups.')

    @with_argparser(list_parser)
    def do_list(self, args):
        if args.nvmesh_object == 'targets':
            list_targets(args.details, args.tsv, args.json, args.servers)
            return
        elif args.nvmesh_object == 'clients':
            list_clients(args.tsv, args.servers)
            return
        elif args.nvmesh_object == 'volumes':
            list_volumes(args.details, args.tsv, args.volumes)
        elif args.nvmesh_object == 'sshuser':
            print(user.SSH_user_name)
        elif args.nvmesh_object == 'apiuser':
            print(user.API_user_name)
        elif args.nvmesh_object == 'manager':
            print(mgmt.server)
        elif args.nvmesh_object == 'cluster':
            list_cluster(args.tsv, args.json)
        elif args.nvmesh_object == 'vpgs':
            list_vpgs(args.tsv, args.json, args.vpgs)
        elif args.nvmesh_object == 'driveclasses':
            list_drive_classes(args.details, args.tsv, args.json, args.classes)
        elif args.nvmesh_object == 'targetclasses':
            list_target_classes(args.tsv, args.json, args.classes)

    add_parser = argparse.ArgumentParser()
    add_parser.add_argument('nvmesh_object', choices=['hosts'],
                             nargs=1,
                             help='Add hosts to this shell environment')
    add_parser.add_argument('-t', '--persistent', required=False, action='store_const', const=True,
                            help='Add the NVMesh runtime variable persistently.')
    add_parser.add_argument('-s', '--servers', nargs='+', required=False,
                             help='Specify a single server or a list of servers.')

    @with_argparser(add_parser)
    def do_add(self, args):
        if args.nvmesh_object == 'manager':
            mgmt.save_management_server(args.servers)

    check_parser = argparse.ArgumentParser()
    check_parser.add_argument('nvmesh_object', choices=['clients', 'targets', 'managers', 'cluster'],
                            nargs='?', default='cluster',
                            help='Check the NVMesh services via SSH')
    check_parser.add_argument('-d', '--details', required=False, action='store_const', const=True,
                            help='List and view the service details.')
    check_parser.add_argument('-s', '--server', nargs='+', required=False,
                            help='Specify a single or a list of servers.')

    @with_argparser(check_parser)
    def do_check(self, args):
        if args.nvmesh_object == 'targets':
            check_targets(args.details, args.server)
        elif args.nvmesh_object == 'clients':
            check_clients(args.details, args.server)
        elif args.nvmesh_object == 'managers':
            check_managers(args.details, args.server)
        elif args.nvmesh_object == 'cluster':
            check_cluster()

    stop_parser = argparse.ArgumentParser()
    stop_parser.add_argument('nvmesh_object', choices=['clients', 'targets', 'managers', 'cluster', 'mcm'],
                              nargs='?', default='cluster',
                              help='Stop the NVMesh services.')
    stop_parser.add_argument('-d', '--details', required=False, action='store_const', const=True,
                              help='List and view the service details.')
    stop_parser.add_argument('-f', '--force', required=False, action='store_const', const=True,
                             help='Force the stopping of the service.')
    stop_parser.add_argument('-s', '--server', nargs='+', required=False,
                              help='Specify a single or a list of servers.')

    @with_argparser(stop_parser)
    def do_stop(self, args):
        if args.nvmesh_object == 'targets':
            stop_targets(args.details, args.server, args.force)
        elif args.nvmesh_object == 'clients':
            stop_clients(args.details, args.server)
        elif args.nvmesh_object == 'managers':
            stop_managers(args.details, args.server)
        elif args.nvmesh_object == 'cluster':
            stop_cluster(args.details, args.force)
        elif args.nvmesh_object == 'mcm':
            stop_mcm(args.server)

    start_parser = argparse.ArgumentParser()
    start_parser.add_argument('nvmesh_object', choices=['clients', 'targets', 'managers', 'cluster', 'mcm'],
                             nargs='?', default='cluster',
                             help='Check the NVMesh services via SSH')
    start_parser.add_argument('-d', '--details', required=False, action='store_const', const=True,
                             help='List and view the service details.')
    start_parser.add_argument('-s', '--server', nargs='+', required=False,
                             help='Specify a single or a list of servers.')

    @with_argparser(start_parser)
    def do_start(self, args):
        if args.nvmesh_object == 'targets':
            start_targets(args.details, args.server)
        elif args.nvmesh_object == 'clients':
            start_clients(args.details, args.server)
        elif args.nvmesh_object == 'managers':
            start_managers(args.details, args.server)
        elif args.nvmesh_object == 'cluster':
            start_cluster(args.details)
        elif args.nvmesh_object == 'mcm':
            start_mcm(args.server)

    restart_parser = argparse.ArgumentParser()
    restart_parser.add_argument('nvmesh_object', choices=['clients', 'targets', 'managers', 'cluster', 'mcm'],
                              nargs='?', default='cluster',
                              help='Restart the NVMesh services.')
    restart_parser.add_argument('-d', '--details', required=False, action='store_const', const=True,
                              help='List and view the service details.')
    restart_parser.add_argument('-s', '--server', nargs='+', required=False,
                              help='Specify a single or a list of servers.')

    @with_argparser(start_parser)
    def do_restart(self, args):
        if args.nvmesh_object == 'targets':
            restart_targets(args.details, args.server)
        elif args.nvmesh_object == 'clients':
            restart_clients(args.details, args.server)
        elif args.nvmesh_object == 'managers':
            restart_managers(args.details, args.server)
        elif args.nvmesh_object == 'mcm':
            restart_mcm(args.server)

    define_parser = argparse.ArgumentParser()
    define_parser.add_argument('nvmesh_object', choices=['manager','sshuser', 'sshpassword', 'apiuser', 'apipassword'],
                            nargs='?',
                            help='Define the NVMesh shell runtime variables')
    define_parser.add_argument('-t', '--persistent', required=False, action='store_const', const=True,
                            help='Define/Set the NVMesh runtime variable persistently.')
    define_parser.add_argument('-p', '--password', nargs=1, required=False,
                             help='Provide the password for the user to be defined.')
    define_parser.add_argument('-u', '--user', nargs=1, required=False,
                               help='Provide the password for the user to be defined.')
    define_parser.add_argument('-s', '--server', nargs='+', required=False,
                            help='Define/Set the NVMesh management server')

    @with_argparser(define_parser)
    def do_define(self, args):
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

    def do_license(self, args):
        """Shows the licensing details, term and conditions. """
        print open('LICENSE.txt', 'r').read()


def start_shell():
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
        print '''
Copyright (c) 2018 Excelero, Inc. All rights reserved.

This program comes with ABSOLUTELY NO WARRANTY; for licencing and warranty details type 'license'.
This is free software, and you are welcome to redistribute it under certain conditions; type 'license' for details.
        '''
        shell.cmdloop('Starting NVMesh Shell ...')


if __name__ == '__main__':
    start_shell()
