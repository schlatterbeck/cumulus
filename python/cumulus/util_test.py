#!/usr/bin/python
# coding: utf-8
#
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

"""Unit tests for the cumulus.util module."""

from __future__ import division, print_function, unicode_literals

import six
import unittest

from cumulus import util

class UtilCodecs(unittest.TestCase):
    def test_pathnames(self):
        self.assertEqual(util.ENCODING, "utf-8")
        if six.PY2:
            self.assertEqual(util.bytes_to_pathname(b"ext\xc3\xa9nsion"),
                             b"ext\xc3\xa9nsion")
            self.assertEqual(util.pathname_to_bytes(b"ext\xc3\xa9nsion"),
                             b"ext\xc3\xa9nsion")
            self.assertEqual(util.pathname_to_bytes(u"exténsion"),
                             b"ext\xc3\xa9nsion")
        elif six.PY3:
            self.assertEqual(util.bytes_to_pathname(b"ext\xc3\xa9nsion"),
                             "exténsion")
            self.assertEqual(util.pathname_to_bytes("exténsion"),
                             b"ext\xc3\xa9nsion")
            self.assertEqual(util.bytes_to_pathname(b"inv\xe1lid"),
                             "inv\udce1lid")
            self.assertEqual(util.pathname_to_bytes("inv\udce1lid"),
                             b"inv\xe1lid")

    def test_uri_encode_raw(self):
        self.assertEqual(util.uri_encode_raw(b"sample ASCII"), "sample%20ASCII")
        self.assertEqual(util.uri_encode_raw(b"sample ext\xc3\xa9nded"),
                         "sample%20ext%c3%a9nded")

    def test_uri_decode_raw(self):
        self.assertEqual(util.uri_decode_raw("sample%20ASCII"), b"sample ASCII")
        self.assertEqual(util.uri_decode_raw("sample%20ext%c3%a9nded"),
                         b"sample ext\xc3\xa9nded")

    def test_uri_decode_pathname(self):
        if six.PY2:
            self.assertEqual(util.uri_decode_pathname("sample%20ext%c3%a9nded"),
                             b"sample ext\xc3\xa9nded")
            self.assertEqual(util.uri_decode_pathname("sample%20exténded"),
                             b"sample ext\xc3\xa9nded")
            # In Python 2, non-UTF-8 sequences are just passed through as
            # bytestrings.
            self.assertEqual(util.uri_decode_pathname(b"inv%e1lid"),
                             b"inv\xe1lid")
            self.assertEqual(util.uri_decode_pathname(b"inv\xe1lid"),
                             b"inv\xe1lid")
        elif six.PY3:
            self.assertEqual(util.uri_decode_pathname("sample%20ext%c3%a9nded"),
                             "sample exténded")
            self.assertEqual(util.uri_decode_pathname("sample%20exténded"),
                             "sample exténded")
            # In Python 3, non-UTF-8 sequences are represented using surrogate
            # escapes to allow lossless conversion back to the appropriate
            # bytestring.
            self.assertEqual(util.uri_decode_pathname("inv%e1lid"),
                             "inv\udce1lid")
            self.assertEqual(
                util.pathname_to_bytes(util.uri_decode_pathname("inv%e1lid")),
                b"inv\xe1lid")


if __name__ == "__main__":
    unittest.main()
