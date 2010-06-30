# Amazon S3 storage backend.  Uses a URL of the form s3://BUCKET/PATH/.
import os, sys, tempfile
import boto
from boto.s3.bucket import Bucket
from boto.s3.key import Key

import cumulus.store

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
