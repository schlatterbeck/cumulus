# Cumulus: Efficient Filesystem Backup to the Cloud
# Copyright (C) 2012 The Cumulus Developers
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

"""Parsing of Cumulus backup configuration files.

See the Cumulus documentation for a description of the configuration file
format.
"""

import ConfigParser
import datetime
import re

from cumulus import retention

_BACKUP_PREFIX = "backup:"
_TIME_UNITS = {"s": 1, "m": 60, "h": 3600, "D": 86400, "W": 7 * 86400,
               "M": 30 * 86400, "Y": 365 * 86400}
_INTERVAL_RE = r"(\d+)([smhDWMY])"

def _build_retention_engine(spec):
    """Parse a retention specification and return a RetentionEngine object."""
    policy = retention.RetentionEngine()
    class_re = re.compile(r"^(\w+):((%s)+)$" % _INTERVAL_RE)
    interval_re = re.compile(r"^%s(.*)$" % _INTERVAL_RE)
    for s in spec.split():
        m = class_re.match(s)
        if not m:
            print "Invalid retain spec:", s
            continue
        period = datetime.timedelta()
        classname = m.group(1)
        intervalspec = m.group(2)
        while intervalspec:
            m = interval_re.match(intervalspec)
            seconds = int(m.group(1)) * _TIME_UNITS[m.group(2)]
            period = period + datetime.timedelta(seconds=seconds)
            intervalspec = m.group(3)
        print classname, period
        policy.add_policy(classname, period)
    return policy


class CumulusConfig(object):
    def __init__(self, filename):
        """Parse a Cumulus backup configuration from the specified file."""
        self._config = ConfigParser.RawConfigParser()
        self._config.readfp(open(filename))

    def get_global(self, key):
        return self._config.get("global", key)

    def backup_schemes(self):
        """Returns a list of backup schemes."""
        return [s[len(_BACKUP_PREFIX):] for s in self._config.sections()
                if s.startswith(_BACKUP_PREFIX)]

    def get_retention_for_scheme(self, scheme):
        spec = self._config.get(_BACKUP_PREFIX + scheme, "retain")
        return _build_retention_engine(spec)
