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
