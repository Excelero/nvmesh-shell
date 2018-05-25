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
import humanfriendly
import constants


class Api:
    def __init__(self):
        self.api_protocol = 'https'
        self.api_server = None
        self.api_port = '4000'
        self.api_user_name = None
        self.api_password = None
        self.api_endpoint = None
        self.api_payload = {}
        self.api_session = requests.session()
        self.api_response = None
        self.api_session.verify = False
        self.err = None

    def login(self):
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.api_endpoint = '/login'
        self.api_payload = {
            "username" : self.api_user_name,
            "password" : self.api_password
        }
        self.api_response = self.api_session.post('%s://%s:%s%s' % (self.api_protocol, self.api_server,
                                                                    self.api_port, self.api_endpoint),
                                                  json=self.api_payload)

    def get_cluster(self):
        try:
            self.api_endpoint = '/status'
            self.api_response = self.api_session.get("%s://%s:%s%s" % (self.api_protocol, self.api_server,
                                                                       self.api_port, self.api_endpoint))
            return self.api_response.content
        except Exception, e:
            return e.message

    def get_space_allocation(self):
        try:
            self.api_endpoint = '/getSpaceAllocation'
            self.api_response = self.api_session.get("%s://%s:%s%s" % (self.api_protocol, self.api_server,
                                                                       self.api_port, self.api_endpoint))
            return self.api_response.content
        except Exception, e:
            return e.message

    def get_servers(self):
        try:
            self.api_endpoint = '/servers/all/%s/%s' % (0, 0)
            self.api_response = self.api_session.get("%s://%s:%s%s" % (self.api_protocol, self.api_server,
                                                                       self.api_port, self.api_endpoint))
            return self.api_response.content
        except Exception, e:
            return e.message

    def get_clients(self):
        try:
            self.api_endpoint = '/clients/all/%s/%s' % (0, 0)
            self.api_response = self.api_session.get("%s://%s:%s%s" % (self.api_protocol, self.api_server,
                                                                       self.api_port, self.api_endpoint))
            return self.api_response.content
        except Exception, e:
            return e.message

    def get_volumes(self):
        try:
            self.api_endpoint = '/volumes/all/%s/%s' % (0, 0)
            self.api_response = self.api_session.get("%s://%s:%s%s" % (self.api_protocol, self.api_server,
                                                                       self.api_port, self.api_endpoint))
            return self.api_response.content
        except Exception, e:
            return e.message

    def get_cluster_status(self):
        try:
            self.api_endpoint = '/status'
            self.api_response = self.api_session.get("%s://%s:%s%s" % (self.api_protocol, self.api_server,
                                                                       self.api_port, self.api_endpoint))
            return self.api_response.content
        except Exception, e:
            return e.message

    def get_cluster_logs(self):
        try:
            self.api_endpoint = '/logs/all'
            self.api_response = self.api_session.get("%s://%s:%s%s" % (self.api_protocol, self.api_server,
                                                                       self.api_port, self.api_endpoint))
            return self.api_response.content
        except Exception, e:
            return e.message

    def get_vpgs(self):
        try:
            self.api_endpoint = '/volumeProvisioningGroups/all'
            self.api_response = self.api_session.get("%s://%s:%s%s" % (self.api_protocol, self.api_server,
                                                                       self.api_port, self.api_endpoint))
            return self.api_response.content
        except Exception, e:
            return e.message

    def get_disk_classes(self):
        try:
            self.api_endpoint = '/diskClasses/all'
            self.api_response = self.api_session.get("%s://%s:%s%s" % (self.api_protocol, self.api_server,
                                                                       self.api_port, self.api_endpoint))
            return self.api_response.content
        except Exception, e:
            return e.message

    def get_target_classes(self):
        try:
            self.api_endpoint = '/serverClasses/all'
            self.api_response = self.api_session.get("%s://%s:%s%s" % (self.api_protocol, self.api_server,
                                                                       self.api_port, self.api_endpoint))
            return self.api_response.content
        except Exception, e:
            return e.message

    def target_cluster_shutdown(self):
        try:
            self.api_endpoint = '/servers/setBatchControlJobs'
            self.api_payload = {
                "control": "shutdownAll"
            }
            self.api_response = self.api_session.post("%s://%s:%s%s" % (self.api_protocol, self.api_server,
                                                                    self.api_port, self.api_endpoint),
                                                      json=self.api_payload)
            return self.api_response.content
        except Exception, e:
            return e.message

    def create_volume(self, arguments):
        self.api_endpoint = '/volumes/save'

        payload = {
            'name': arguments['name'],
            'capacity': 'MAX' if str(arguments['capacity']).upper() == 'MAX' else humanfriendly.parse_size(
                arguments['capacity'], binary=True),
        }

        if 'description' in arguments:
            payload['description'] = arguments['description']

        if 'diskClasses' in arguments:
            payload['diskClasses'] = arguments['diskClasses']

        if 'serverClasses' in arguments:
            payload['serverClasses'] = arguments['serverClasses']

        if 'limitByNodes' in arguments:
            payload['limitByNodes'] = arguments['limitByNodes']

        if 'limitByDisks' in arguments:
            payload['limitByDisks'] = arguments['limitByDisks']

        if 'awareness' in arguments:
            payload['awareness'] = arguments['awareness']

        if 'RAIDLevel' in arguments and 'VPG' not in arguments:
            payload['RAIDLevel'] = arguments['RAIDLevel']

            if arguments['RAIDLevel'] == constants.RAID_LEVELS['lvm']:
                pass

            elif arguments['RAIDLevel'] == constants.RAID_LEVELS['r0']:
                payload['stripeSize'] = 32
                payload['stripeWidth'] = arguments['stripeWidth']

            elif arguments['RAIDLevel'] == constants.RAID_LEVELS['1']:
                payload['numberOfMirrors'] = arguments['numberOfMirrors'] if 'numberOfMirrors' in arguments else 1

            elif arguments['RAIDLevel'] == constants.RAID_LEVELS['r10']:
                payload['stripeSize'] = 32
                payload['stripeWidth'] = arguments['stripeWidth']
                payload['numberOfMirrors'] = arguments['numberOfMirrors']

        if 'VPG' in arguments:
            payload['VPG'] = arguments['VPG']

        self.api_payload['create'] = [payload]
        self.api_payload['remove'] = []
        self.api_payload['edit'] = []
        self. err, self.api_response = self.api_session.post('%s://%s:%s%s' % (self.api_protocol, self.api_server,
                                                                               self.api_port, self.api_endpoint),
                                                             json=self.api_payload)
        return self.err, self.api_response
