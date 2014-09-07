# Cumulus: Efficient Filesystem Backup to the Cloud
# Copyright (C) 2014 The Cumulus Developers
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

"""Collected utility functions for use by Cumulus."""

from __future__ import division, print_function, unicode_literals

import re
import six

# The encoding assumed when interpreting path names.
ENCODING="utf-8"

# In both Python 2 and Python 3 pathnames are represented using the str type.
# For Python 2, this means that the converting from a bytestring to a pathname
# is a no-op.  For Python 3, the conversion assumes a utf-8 encoding, but the
# surrogateescape encoding error handler is used to allow other byte sequences
# to be passed through.
if six.PY2:
    def bytes_to_pathname(b): return b
    def pathname_to_bytes(p):
        if isinstance(p, unicode):
            return p.encode(encoding=ENCODING, errors="replace")
        else:
            return p
elif six.PY3:
    def bytes_to_pathname(b):
        """Decodes a byte string to a pathname.

        The input is assumed to be encoded using ENCODING (defaults to
        utf-8)."""
        return b.decode(encoding=ENCODING, errors="surrogateescape")

    def pathname_to_bytes(p):
        """Converts a pathname to encoded bytes.

        The input is encoded to ENCODING (defaults to utf-8)."""
        return p.encode(encoding=ENCODING, errors="surrogateescape")
else:
    raise AssertionError("Unsupported Python version")

def uri_decode_raw(s):
    """Decode a URI-encoded (%xx escapes) string.

    The input should be a string, preferably only using ASCII characters.  The
    output will be of type bytes."""
    def hex_decode(m): return six.int2byte(int(m.group(1), 16))
    return re.sub(br"%([0-9a-fA-F]{2})", hex_decode, pathname_to_bytes(s))

def uri_encode_raw(s):
    """Encode a bytes array to URI-encoded (%xx escapes) form."""
    def hex_encode(c):
        # Allow certain literal characters: c > "+" and c < "\x7f" and c != "@"
        if c > 0x2b and c < 0x7f and c != 0x40:
            return chr(c)
        else:
            return "%%%02x" % c

    return "".join(hex_encode(c) for c in six.iterbytes(s))

def uri_decode_pathname(s):
    """Decodes a URI-encoded string to a pathname."""
    return bytes_to_pathname(uri_decode_raw(s))

def uri_encode_pathname(p):
    """Encodes a pathname to a URI-encoded string."""
    return uri_encode_raw(pathname_to_bytes(p))
