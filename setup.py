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

from setuptools import setup

setup(
    name='nvmesh-shell',
    version='0.1',
    py_modules=['nvmesh-shell', 'constants', 'nvmesh_api'],
    install_requires=['Cmd2', 'paramiko', 'humanfriendly', 'gnureadline'],
    entry_points="""
        [console_scripts]
        nvmesh-shell=nvmesh_shell:start_shell
        """,
)
