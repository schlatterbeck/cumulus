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

"""Backup retention policies.

Retention policies control how long different backup snapshots should be kept,
for example keeping daily snapshots for short periods of time but retaining
weekly snapshots going back further in time.
"""

import calendar
import datetime

TIMESTAMP_FORMAT = "%Y%m%dT%H%M%S"

# Different classes of backups--such as "daily" or "monthly"--can have
# different retention periods applied.  A single backup snapshot might belong
# to multiple classes (i.e., perhaps be both a "daily" and a "monthly", though
# not a "weekly").
#
# Backups are classified using partitioning functions, defined below.  For a
# "monthly" backup classifier, all backups for a given month should map to the
# same partition.  Then, we apply the class label to the earliest snapshot in
# each partition--so the set of "monthly" backups would consist of all backups
# which were the first to run after the start of a month.
#
# A partitioning function must take a datetime instance as input and return a
# partition representative as output; timestamps that should be part of the
# same partition should map to equal partition representatives.  For a
# "monthly" classifier, an easy way to do this is to truncate the timestamp to
# keep only the month and year, and in general truncating timestamps works
# well, but the values are not used in any other way than equality testing so
# any type is allowed.
#
# _backup_classes is a registry of useful backup types; it maps a descriptive
# name to a partition function which implements it.
_backup_classes = {}

def add_backup_class(name, partioning_function):
    """Registers a new class of backups for which policies can be applied.

    The new class will be available as name to RetentionEngine.add_policy.
    partioning_function should be a function for grouping together backups in
    the same time period.

    Predefined backups classes are: "yearly", "monthly", "weekly", "daily", and
    "all".
    """
    _backup_classes[name] = partioning_function

add_backup_class("yearly", lambda t: t.date().replace(day=1, month=1))
add_backup_class("monthly", lambda t: t.date().replace(day=1))
add_backup_class("weekly", lambda t: t.isocalendar()[0:2])
add_backup_class("daily", lambda t: t.date())
add_backup_class("all", lambda t: t)


class RetentionEngine(object):
    """Class for applying a retention policy to a set of snapshots.

    Allows a retention policy to be set, then matches a sequence of backup
    snapshots to the policy to decide which ones should be kept.
    """

    def __init__(self):
        self.set_utc(False)
        self._policies = {}
        self._last_snapshots = {}
        self._now = datetime.datetime.utcnow()

    def set_utc(self, use_utc=True):
        """Perform policy matching with timestamps in UTC.

        By default, the policy converts timestamps to local time, but calling
        set_utc(True) will select snapshots based on UTC timestamps.
        """
        self._convert_to_localtime = not use_utc

    def set_now(self, timestamp):
        """Sets the "current time" for the purposes of snapshot expiration.

        timestamp should be a datetime object, expressed in UTC.  If set_now()
        is not called, the current time defaults to the time at which the
        RetentionEngine object was instantiated.
        """
        self._now = timestamp

    def add_policy(self, backup_class, retention_period):
        self._policies[backup_class] = retention_period
        self._last_snapshots[backup_class] = (None, None, False)

    @staticmethod
    def parse_timestamp(s):
        if isinstance(s, datetime.datetime):
            return s
        return datetime.datetime.strptime(s, TIMESTAMP_FORMAT)

    def consider_snapshot(self, snapshot):
        """Compute whether a given snapshot should be expired.

        Successive calls to consider_snapshot() must be for snapshots in
        chronological order.  For each call, consider_snapshot() will return a
        boolean indicating whether the snapshot should be retained (True) or
        expired (False).
        """
        timestamp_utc = self.parse_timestamp(snapshot)
        snapshot_age = self._now - timestamp_utc

        # timestamp_policy is the timestamp in the format that will be used for
        # doing policy matching: either in the local timezone or UTC, depending
        # on the setting of set_utc().
        if self._convert_to_localtime:
            unixtime = calendar.timegm(timestamp_utc.timetuple())
            timestamp_policy = datetime.datetime.fromtimestamp(unixtime)
        else:
            timestamp_policy = timestamp_utc

        self._labels = set()
        retain = False
        for (backup_class, retention_period) in self._policies.iteritems():
            partition = _backup_classes[backup_class](timestamp_policy)
            last_snapshot = self._last_snapshots[backup_class]
            if self._last_snapshots[backup_class][0] != partition:
                self._labels.add(backup_class)
                retain_label = snapshot_age < retention_period
                self._last_snapshots[backup_class] = (partition, snapshot,
                                                      retain_label)
                if retain_label: retain = True
        return retain

    def last_labels(self):
        """Return the set of policies that applied to the last snapshot.

        This will fail if consider_snapshot has not yet been called.
        """
        return self._labels

    def last_snapshots(self):
        """Returns the most recent snapshot in each backup class."""
        return dict((k, v[1]) for (k, v)
                    in self._last_snapshots.iteritems() if v[2])
