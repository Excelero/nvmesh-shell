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
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md')) as f:
    long_description = f.read()

setup(
    name='nvmesh-shell',
    version='47',
    author='Excelero, Inc. - Andreas Krause',
    url='https://github.com/Excelero/nvmesh-shell',
    description='Excelero NVMesh interactive shell and cli tool.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author_email='andreas@excelero.com',
    classifiers=[
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 5 - Production/Stable',
        'Programming Language :: Python :: 2.7',
    ],
    project_urls={
        'Bug Reports': 'https://github.com/Excelero/nvmesh-shell/issues',
        'Source': 'https://github.com/Excelero/nvmesh-shell/',
        'Documentation': 'https://github.com/Excelero/nvmesh-shell/wiki',
        'GPLv3 License': 'https://www.gnu.org/licenses/gpl-3.0.en.html',
    },
    py_modules=['nvmesh',],
    install_requires=['cmd2',
                      'paramiko',
                      'humanfriendly',
                      'gnureadline',
                      'requests',
                      'urllib3',
                      'ipython',
                      'python-dateutil'],

    entry_points="""
        [console_scripts]
        nvmesh=nvmesh:start_shell
        """,
)
