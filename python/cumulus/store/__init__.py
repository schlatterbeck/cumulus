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

import exceptions, re, urlparse

type_patterns = {
    'checksums': re.compile(r"^snapshot-(.*)\.(\w+)sums$"),
    'segments': re.compile(r"^([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(\.\S+)?$"),
    'snapshots': re.compile(r"^snapshot-(.*)\.(cumulus|lbs)$")
}

class NotFoundError(exceptions.KeyError):
    """Exception thrown when a file is not found in a repository."""

    pass

class Store (object):
    """Base class for all cumulus storage backends."""

    def __new__ (cls, url, **kw):
        """ Return the correct sub-class depending on url,
        pass parsed url parameters to object
        """
        if cls != Store:
            return super(Store, cls).__new__(cls, url, **kw)
        (scheme, netloc, path, params, query, fragment) \
            = urlparse.urlparse(url)

        try:
            cumulus = __import__('cumulus.store.%s' % scheme, globals())
            subcls = getattr (cumulus.store, scheme).Store
            obj = super(Store, cls).__new__(subcls, url, **kw)
            obj.scheme = scheme
            obj.netloc = netloc
            obj.path = path
            obj.params = params
            obj.query = query
            obj.fragment = fragment
            return obj
        except ImportError:
            raise NotImplementedError, "Scheme %s not implemented" % scheme

    def list(self, type):
        raise NotImplementedError

    def get(self, type, name):
        raise NotImplementedError

    def put(self, type, name, fp):
        raise NotImplementedError

    def delete(self, type, name):
        raise NotImplementedError

    def stat(self, type, name):
        raise NotImplementedError

    def scan(self):
        """Cache file information stored in this backend.

        This might make subsequent list or stat calls more efficient, but this
        function is intended purely as a performance optimization."""

        pass

    def close(self):
        """Tear down the connection explicitly if needed

        Currently needed for sftp to be able to end the program."""

        pass

    def __del__(self):
        self.close()

def open(url):
    return Store(url)
