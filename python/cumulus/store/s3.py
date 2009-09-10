import os, sys, tempfile
import boto
from boto.s3.bucket import Bucket
from boto.s3.key import Key

import cumulus.store

class S3Store(cumulus.store.Store):
    def __init__(self, url, **kw):
        (bucket, prefix) = self.path.lstrip("/").split("/", 1)
        self.conn = boto.connect_s3(is_secure=False)
        self.bucket = self.conn.create_bucket(bucket)
        self.prefix = prefix.rstrip ("/")
        self.scan_cache = {}

    def _get_key(self, type, name):
        k = Key(self.bucket)
        k.key = "%s/%s/%s" % (self.prefix, type, name)
        return k

    def scan(self):
        prefix = "%s/" % (self.prefix,)
        for i in self.bucket.list(prefix):
            assert i.key.startswith(prefix)
            self.scan_cache[i.key] = i

    def list(self, type):
        prefix = "%s/%s/" % (self.prefix, type)
        for i in self.bucket.list(prefix):
            assert i.key.startswith(prefix)
            yield i.key[len(prefix):]

    def get(self, type, name):
        fp = tempfile.TemporaryFile()
        k = self._get_key(type, name)
        k.get_file(fp)
        fp.seek(0)
        return fp

    def put(self, type, name, fp):
        k = self._get_key(type, name)
        k.set_contents_from_file(fp)

    def delete(self, type, name):
        self.bucket.delete_key("%s/%s/%s" % (self.prefix, type, name))

    def stat(self, type, name):
        path = "%s/%s/%s" % (self.prefix, type, name)
        if path in self.scan_cache:
            k = self.scan_cache[path]
        else:
            k = self.bucket.get_key(path)
        if k is None:
            raise cumulus.store.NotFoundError

        return {'size': int(k.size)}

Store = S3Store
