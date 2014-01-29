# Cumulus: Efficient Filesystem Backup to the Cloud
# Copyright (C) 2008-2010 The Cumulus Developers
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

"""Amazon S3 storage backend.  Uses a URL of the form s3://BUCKET/PATH/."""

from __future__ import division, print_function, unicode_literals

import os, sys, tempfile
import boto
from boto.exception import S3ResponseError
from boto.s3.bucket import Bucket
from boto.s3.key import Key

import cumulus.store

def throw_notfound(method):
    """Decorator to convert a 404 error into a cumulus.store.NoutFoundError."""
    def f(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except S3ResponseError as e:
            if e.status == 404:
                raise cumulus.store.NotFoundError(e)
            else:
                raise
    return f

class Store(cumulus.store.Store):
    def __init__(self, url):
        super(Store, self).__init__(url)
        self.conn = boto.connect_s3(is_secure=False)
        self.bucket = self.conn.create_bucket(url.hostname)
        self.prefix = url.path
        if not self.prefix.endswith("/"):
            self.prefix += "/"
        self.prefix = self.prefix.lstrip("/")
        self.scan_cache = {}

    def _fullpath(self, path, is_directory=False):
        fullpath = self.prefix + path
        if is_directory and not fullpath.endswith("/"):
            fullpath += "/"
        return fullpath

    def _get_key(self, path):
        k = Key(self.bucket)
        k.key = self._fullpath(path)
        return k

    @throw_notfound
    def scan(self, path):
        prefix = self._fullpath(path, is_directory=True)
        for i in self.bucket.list(prefix):
            assert i.key.startswith(prefix)
            self.scan_cache[i.key] = i

    @throw_notfound
    def list(self, path):
        prefix = self._fullpath(path, is_directory=True)
        # TODO: Should use a delimiter
        for i in self.bucket.list(prefix):
            assert i.key.startswith(prefix)
            yield i.key[len(prefix):]

    @throw_notfound
    def get(self, path):
        fp = tempfile.TemporaryFile()
        k = self._get_key(path)
        k.get_file(fp)
        fp.seek(0)
        return fp

    @throw_notfound
    def put(self, path, fp):
        k = self._get_key(path)
        k.set_contents_from_file(fp)

    @throw_notfound
    def delete(self, path):
        self.bucket.delete_key(self._fullpath(path))

    def stat(self, path):
        path = self._fullpath(path)
        if path in self.scan_cache:
            k = self.scan_cache[path]
        else:
            k = self.bucket.get_key(path)
        if k is None:
            raise cumulus.store.NotFoundError

        return {'size': int(k.size)}
