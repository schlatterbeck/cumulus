# Cumulus: Efficient Filesystem Backup to the Cloud
# Copyright (C) 2008-2009 The Cumulus Developers
# See the AUTHORS file for a list of contributors.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

from __future__ import division, print_function, unicode_literals

import os, sys, tempfile

import cumulus.store

class Store(cumulus.store.Store):
    """Storage backend that accesses the local file system."""
    def __init__(self, url):
        super(Store, self).__init__(url)
        self.prefix = cumulus.store.unquote(url.path)

    def list(self, subdir):
        try:
            return os.listdir(os.path.join(self.prefix, subdir))
        except OSError:
            raise cumulus.store.NotFoundError(subdir)

    def get(self, path):
        try:
            return open(os.path.join(self.prefix, path), 'rb')
        except IOError:
            raise cumulus.store.NotFoundError(path)

    def put(self, path, fp):
        with open(os.path.join(self.prefix, path), "wb") as out:
            buf = fp.read(4096)
            while len(buf) > 0:
                out.write(buf)
                buf = fp.read(4096)

    def delete(self, path):
        os.unlink(os.path.join(self.prefix, path))

    def stat(self, path):
        try:
            stat = os.stat(os.path.join(self.prefix, path))
            return {'size': stat.st_size}
        except OSError:
            raise cumulus.store.NotFoundError(path)
