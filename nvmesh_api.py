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

import requests
import urllib3
import logger
import datetime
import os


class Api:
    def __init__(self):
        self.protocol = 'https'
        self.server = None
        self.port = '4000'
        self.user_name = None
        self.password = None
        self.endpoint = None
        self.payload = {}
        self.session = requests.session()
        self.response = None
        self.session.verify = False
        self.err = None
        self.action = None

    def execute_api_call(self):
        if self.action == "post":
            logger.log("debug",
                       "API action: POST %s://%s:%s%s" % (self.protocol, self.server, self.port, self.endpoint))
            logger.log("debug", "API payload: %s" % self.payload if '/login' not in self.endpoint else 'login')
            self.response = self.session.post(
                '%s://%s:%s%s' % (self.protocol, self.server, self.port, self.endpoint), json=self.payload)
            logger.log("debug", "API response: %s" % self.response)
            logger.log(
                "debug",
                "API response content is: %s" % self.response.content if '/login' not in self.endpoint else 'login')
            return self.response.content
        elif self.action == "get":
            logger.log("debug",
                       "API action: GET %s://%s:%s%s" % (self.protocol, self.server, self.port, self.endpoint))
            self.response = self.session.get(
                "%s://%s:%s%s" % (self.protocol, self.server, self.port, self.endpoint))
            logger.log("debug", "API response status code: %s" % self.response)
            logger.log("debug", "API response content is: %s" % self.response.content)
            return self.response.content

    def login(self):
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.action = "post"
        self.endpoint = '/login'
        self.payload = {
            "username": self.user_name,
            "password": self.password
        }
        return self.execute_api_call()

    def get_cluster(self):
        self.endpoint = '/status'
        self.action = "get"
        return self.execute_api_call()

    def get_space_allocation(self):
        self.endpoint = '/getSpaceAllocation'
        self.action = "get"
        return self.execute_api_call()

    def get_servers(self):
        self.endpoint = '/servers/all/%s/%s' % (0, 0)
        self.action = "get"
        return self.execute_api_call()

    def get_clients(self):
        self.endpoint = '/clients/all/%s/%s' % (0, 0)
        self.action = "get"
        return self.execute_api_call()

    def get_volumes(self):
        self.endpoint = '/volumes/all/%s/%s' % (0, 0)
        self.action = "get"
        return self.execute_api_call()

    def get_cluster_status(self):
        self.endpoint = '/status'
        self.action = "get"
        return self.execute_api_call()

    def get_logs(self, all_logs):
        if all_logs is True:
            self.endpoint = '/logs/all/0/0?filter={}&sort={"timestamp":-1}'
        else:
            self.endpoint = '/logs/alerts/0/0?filter={}&sort={"timestamp":-1}'
        self.action = "get"
        return self.execute_api_call()

    def get_vpgs(self):
        self.endpoint = '/volumeProvisioningGroups/all'
        self.action = "get"
        return self.execute_api_call()

    def get_disk_classes(self):
        self.endpoint = '/diskClasses/all'
        self.action = "get"
        return self.execute_api_call()

    def get_disk_models(self):
        self.endpoint = '/disks/models'
        self.action = "get"
        return self.execute_api_call()

    def get_disk_by_model(self, model):
        self.endpoint = '/disks/disksByModel/%s' % model
        self.action = "get"
        return self.execute_api_call()

    def get_target_classes(self):
        self.endpoint = '/serverClasses/all'
        self.action = "get"
        return self.execute_api_call()

    def get_server_by_id(self, server):
        self.endpoint = '/servers/api/%s' % server
        self.action = "get"
        return self.execute_api_call()

    def target_cluster_shutdown(self):
        self.endpoint = '/servers/setBatchControlJobs'
        self.action = "post"
        return self.execute_api_call()

    def manage_volume(self, payload):
        self.payload = payload
        self.endpoint = '/volumes/save'
        self.action = "post"
        return self.execute_api_call()

    def set_control_jobs(self, payload):
        self.payload = payload
        self.endpoint = '/clients/setControlJobs'
        self.action = "post"
        return self.execute_api_call()

    def manage_drive_class(self, action, payload):
        self.payload = payload
        self.endpoint = '/diskClasses/%s' % action
        self.action = "post"
        return self.execute_api_call()

    def manage_target_class(self, action, payload):
        self.payload = payload
        self.endpoint = '/serverClasses/%s' % action
        self.action = "post"
        return self.execute_api_call()
