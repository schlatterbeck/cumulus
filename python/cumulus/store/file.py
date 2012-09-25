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

import os, sys, tempfile

import cumulus.store

type_patterns = cumulus.store.type_patterns

class FileStore(cumulus.store.Store):
    def __init__(self, url, **kw):
        # if constructor isn't called via factory interpret url as filename
        if not hasattr (self, 'path'):
            self.path = url
        self.prefix = self.path.rstrip("/")

    def _get_path(self, type, name):
        return "%s/%s" % (self.prefix, name)

    def list(self, type):
        files = os.listdir(self.prefix)
        return (f for f in files if type_patterns[type].match(f))

    def get(self, type, name):
        k = self._get_path(type, name)
        return open(k, 'rb')

    def put(self, type, name, fp):
        k = self._get_path(type, name)
        out = open(k, 'wb')
        buf = fp.read(4096)
        while len(buf) > 0:
            out.write(buf)
            buf = fp.read(4096)

    def delete(self, type, name):
        k = self._get_path(type, name)
        os.unlink(k)

    def stat(self, type, name):
        try:
            stat = os.stat(self._get_path(type, name))
            return {'size': stat.st_size}
        except OSError:
            raise cumulus.store.NotFoundError, (type, name)

Store = FileStore
