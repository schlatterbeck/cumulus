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

from __future__ import division, print_function, unicode_literals

import importlib
import re
try:
    # Python 3
    from urllib import parse as urlparse
    from urllib.parse import quote, unquote
except ImportError:
    # Python 2
    from urllib import quote, unquote
    import urlparse

type_patterns = {
    'checksums': re.compile(r"^snapshot-(.*)\.(\w+)sums$"),
    'segments': re.compile(r"^([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(\.\S+)?$"),
    'snapshots': re.compile(r"^snapshot-(.*)\.(cumulus|lbs)$")
}

class NotFoundError(KeyError):
    """Exception thrown when a file is not found in a repository."""

    pass

class Store(object):
    """Base class for all cumulus storage backends."""

    def __init__(self, url):
        """Initializes a new storage backend.

        Params:
          url: The parsed (by urlsplit) URL that specifies the storage
              location.
        """
        pass

    # TODO: Implement context manager.

    def list(self, path):
        raise NotImplementedError

    def get(self, path):
        raise NotImplementedError

    def put(self, path, fp):
        raise NotImplementedError

    def delete(self, path):
        raise NotImplementedError

    def stat(self, path):
        raise NotImplementedError

    def scan(self, path):
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
    """Parse a storage url, then locate and initialize a backend for it."""
    parsed_url = urlparse.urlsplit(url)

    # If there is no scheme, fall back to treating the string as local path and
    # construct a file:/// URL.
    if not parsed_url.scheme:
        parsed_url = urlparse.SplitResult("file", "", quote(url), "", "")

    try:
        # TODO: Support a registry for schemes that don't map to a module.
        if re.match(r"^\w+$", parsed_url.scheme):
            handler = importlib.import_module("cumulus.store.%s" %
                                              parsed_url.scheme)
            obj = handler.Store(parsed_url)
            return obj
    except ImportError:
        # Fall through to error below
        pass

    raise NotImplementedError("Scheme %s not implemented" % scheme)
