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


RAID_LEVELS = {
    'lvm': 'LVM/JBOD',
    '0': 'Striped RAID-0',
    '1': 'Mirrored RAID-1',
    '10': 'Striped & Mirrored RAID-10'
}

CONTROL_JOBS = {
    'attach': 'toBeAttached',
    'detach': 'toBeDetached'
}

REGEX_HCAID = r"(mlx5_\d*)"
REGEX_INSTALLED_MEMORY = r"\S*Mem:\s*(\d*[A-Za-z])"
REGEX_HCA_MAX = r"LnkCap:\s\S*\s\S*\s\S*\s([A-Za-z0-9]*/s),\s\S*\s(\S[0-9]*)"
REGEX_HCA_ACTUAL = r"LnkSta:\s\S*\S*\s([A-Za-z0-9]*/s),\s\S*\s(\S[0-9]*)"
REGEX_HCA_LIST = "(mlx5_\\d*)\\s*node_guid:\\s*([A-Za-z0-9]*):([A-Za-z0-9]*):([A-Za-z0-9]*):([A-Za-z0-9]*)"

EXCELERO_MANAGEMENT_PORTS = [("tcp", 4000), ("tcp", 4001)]
ROCEV2_TARGET_PORT = ("udp", 4791)
MONGODB_PORT = ("tcp", 27017)
RHEL_INBOX_DRIVERS = ["libibverbs", "librdmacm", "libibcm", "libibmad", "libibumad", "libmlx4", "libmlx5", "opensm",
                      "ibutils", "infiniband-diags", "perftest", "mstflint", "rdmacmutils", "ibverbs-utils",
                      "librdmacm-utils", "libibverbs-utils"]
SLES_INBOX_DRIVERS = ["rdma-core", "librdmacm1", "libibmad5", "libibumad3"]
CMD_GET_HOSTNAME = "hostname -f"
CMD_GET_OS_RELEASE = "cat /etc/os-release"
CMD_GET_TUNED_POLICY = "tuned-adm active"
CMD_SET_TUNED_PARAMETERS = "tuned-adm profile latency-performance"
CMD_SET_ONE_QP = "mlxconfig -d %s -b /etc/opt/NVMesh/Excelero_mlxconfig.db set ONE_QP_PER_RECOVERY=1"
CMD_GET_ONE_QP = "mlxconfig -d %s -b ./Excelero_mlxconfig.db query ONE_QP_PER_RECOVERY | grep ONE_QP_PER_RECOVERY"
CMD_DISABLE_FIREWALL = ["systemctl stop firewalld", "systemctl disable firewalld"]
CMD_SET_FIREWALL_FOR_NVMESH_MGMT = ["firewall-cmd --permanent --direct --add-rule ipv4 filter INPUT 0 -p tcp --dport "
                                    "4000 -j ACCEPT -m comment --comment Excelero-Management", "firewall-cmd "
                                                                                               "--permanent --direct --add-rule ipv4 filter INPUT 0 -p tcp --dport 4001 -j "
                                                                                               "ACCEPT -m comment --comment Excelero-Management"]
CMD_SET_FIREWALL_FOR_ROCEV2 = "firewall-cmd --permanent --direct --add-rule ipv4 filter INPUT 0 -p udp --dport " \
                              "4791 -j ACCEPT -m comment --comment RoCEv2-Target"
CMD_SET_FIREWALL_FOR_MOGODB = "firewall-cmd --permanent --direct --add-rule ipv4 filter INPUT 0 -p tcp --dport 27017 " \
                              "-j ACCEPT -m comment --comment MongoDB"
CMD_RELOAD_FIREWALL_RULES = "firewall-cmd --reload"
CMD_GET_IRQ_BALANCER_STATUS = "systemctl status irqbalance"
CMD_START_IRQ_BALANCER = "systemctl start irqbalance"
CMD_ENABALE_IRQ_BALANCER = "systemctl enable irqbalance"
CMD_GET_FIREWALLD_STATUS = "systemctl status firewalld | grep Active"
CMD_GET_FIREWALL_CONFIG = "iptables -nL"
CMD_STOP_SUSE_FIREWALL = "systemctl stop SuSEfirewall2"
CMD_DISABLE_SUSE_FIREWALL = "systemctl disable SuSEfirewall2"

# region Hardware Vendor and System Information Related
CMD_CHECK_FOR_DMIDECODE = "which dmidecode"
CMD_GET_SYSTEM_INFORMATION = "dmidecode | grep -A 4 'System Information'"
CMD_GET_BASE_BOARD_INFORMATION = "dmidecode | grep -A 5 'Base Board Information'"
# endregion

# region SELinux and AppArmor related
CMD_CHECK_FOR_SESTATUS = "which sestatus"
CMD_CHECK_FOR_GETENFORCE = "which getenforce"
CMD_SELINUX_GETENFORCE = "getenforce"
CMD_GET_SETSTATUS = "sestatus"
CMD_DISABLE_SELINUX = "sed -i 's/^SELINUX=enforcing/SELINUX=disabled/' /etc/selinux/config"
CMD_GET_APPARMOR_STATUS = "systemctl status apparmor"
CMD_DISABLE_APPARMOR = "systemctl disable apparmor"
CMD_STOP_APPARMOR = "systemctl stop apparmor"
CMD_GET_APPARMOR_DETAILS = "apparmor_status"
# endregion
CMD_GET_CPU_INFO = "lscpu"
CMD_GET_CPU_FREQ = "dmidecode -s processor-frequency"
CMD_CHECK_FOR_TUNED_ADM = "which tuned-adm"
CMD_INSTALL_TUNED_SLES = "zypper install -y tuned"
CMD_INSTALL_TUNED_RHEL = "yum install -y tuned"

CMD_CHECK_FOR_NVME_CLI = "which nvme"
CMD_GET_NVME_SSD = "nvme list"
CMD_GET_NVME_SDD_NUMA = "lspci -vv | grep -A 10 Volatile | grep -e Volatile -e NUMA"
CMD_INSTALL_NVME_CLI_SLES = "zypper install -y nvme-cli"
CMD_INSTALL_NVME_CLI_RHEL = "yum install -y nvme-cli"

CMD_GET_RNIC_INFO = "for i in `lspci | awk '/Mellanox/ {print $1}'`;do echo $i; echo \"FW level:\" | tr '\n' ' '; cat " \
                    "/sys/bus/pci/devices/0000:$i/infiniband/mlx*_*/fw_ver; lspci -s $i -vvv | egrep -e Connect-X -e " \
                    "\"Product Name:\" -e Subsystem -e NUMA -e \"LnkSta:\" -e \"LnkCap\" -e \"MaxPayload\"; echo """ \
                    "; done"

CMD_GET_OFED_INFO = "ofed_info -n"
CMD_CHECK_RPM = "rpm -q %s"
CMD_GET_IBV_DEVINFO = "ibv_devinfo | grep -e hca_id -e guid"
CMD_GET_IBDEV2NETDEV = "ibdev2netdev -v"
CMD_GET_IBHOSTS = "ibhosts"
CMD_GET_IBSWITCHES = "ibswitches"
CMD_GET_IP_INFO = "ip -4 a s"
CMD_GET_NVMESH_SERVICES = "service --status-all | grep nvmesh"
CMD_GET_NVMESH_SERVICE_DETAILS = "service %s status"
CMD_STOP_NVMESH_SERVICES = "service %s stop"
CMD_CHECK_IF_SERVICE_IS_RUNNING = "systemctl status %s"
CMD_START_TUNED = "systemctl start tuned"
CMD_ENABLE_TUNED = "systemctl enable tuned"
CMD_INSTALL_SLES_PACKAGE = "zypper install -y %s"
CMD_INSTALL_RHEL_PACKAGE = "yum install -y %s"
CMD_STATUS_NVMESH_TARGET = "service nvmeshtarget status"
CMD_STOP_NVMESH_TARGET = "service nvmeshtarget stop"
CMD_START_NVMESH_TARGET = "service nvmeshtarget start"
CMD_STATUS_NVMESH_CLIENT = "service nvmeshclient status"
CMD_STOP_NVMESH_CLIENT = "service nvmeshclient stop"
CMD_START_NVMESH_CLIENT = "service nvmeshclient start"
CMD_STATUS_NVMESH_MANAGER = "service nvmeshmgr status"
CMD_STOP_NVMESH_MANAGER = "service nvmeshmgr stop"
CMD_START_NVMESH_MANAGER = "service nvmeshmgr start"
CMD_RESTART_NVMESH_MANAGER = "service nvmeshmgr restart"
CMD_RESTART_NVMESH_CLIENT = "service nvmeshclient restart"
CMD_RESTART_NVMESH_TARGET = "service nvmeshtarget restart"
CMD_STOP_NVMESH_MCM = "/opt/NVMesh/client-repo/management_cm/managementCM.py stop"
CMD_START_NVMESH_MCM = "/opt/NVMesh/client-repo/management_cm/managementCM.py start"
CMD_RESTART_NVMESH_MCM = "/opt/NVMesh/client-repo/management_cm/managementCM.py restart"
