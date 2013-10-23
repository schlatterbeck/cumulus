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
                print "Got a 404:", e
                raise cumulus.store.NotFoundError(e)
            else:
                raise
    return f

class S3Store(cumulus.store.Store):
    def __init__(self, url, **kw):
        # Old versions of the Python urlparse library will take a URL like
        # s3://bucket/path/ and include the bucket with the path, while new
        # versions (2.6 and later) treat it as the netloc (which seems more
        # correct).
        #
        # But, so that we can work with either behavior, for now just combine
        # the netloc and path together before we do any further processing
        # (which will then split the combined path apart into a bucket and path
        # again).  If we didn't want to support Python 2.5, this would be
        # easier as we could just use the netloc as the bucket directly.
        path = self.netloc + '/' + self.path
        (bucket, prefix) = path.lstrip("/").split("/", 1)
        self.conn = boto.connect_s3(is_secure=False)
        self.bucket = self.conn.create_bucket(bucket)
        self.prefix = prefix.strip("/")
        self.scan_cache = {}

    def _get_key(self, path):
        k = Key(self.bucket)
        k.key = "%s/%s" % (self.prefix, path)
        return k

    @throw_notfound
    def scan(self, path):
        prefix = "%s/%s/" % (self.prefix, path)
        for i in self.bucket.list(prefix):
            assert i.key.startswith(prefix)
            self.scan_cache[i.key] = i

    @throw_notfound
    def list(self, path):
        prefix = "%s/%s/" % (self.prefix, path)
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
        self.bucket.delete_key("%s/%s" % (self.prefix, path))

    def stat(self, path):
        path = "%s/%s" % (self.prefix, path)
        if path in self.scan_cache:
            k = self.scan_cache[path]
        else:
            k = self.bucket.get_key(path)
        if k is None:
            raise cumulus.store.NotFoundError

        return {'size': int(k.size)}

Store = S3Store
