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

"""Python interface to the Cumulus local database of snapshot data.

Includes interfaces for iterating over data and implementing database and
segment cleaning.
"""

from __future__ import division, print_function, unicode_literals

import collections
import datetime
import os
import sqlite3

SnapshotInfo = collections.namedtuple(
    "SnapshotInfo",
    ["id", "scheme", "name", "timestamp"])

SegmentInfo = collections.namedtuple(
    "SegmentInfo",
    ["id", "name", "timestamp", "data_size", "disk_size", "type"])

SegmentStatistics = collections.namedtuple(
    "SegmentStatistics",
    ["id", "name", "timestamp", "data_size", "disk_size", "type",
     "bytes_referenced", "utilization"])

class Database:
    """Access to the local database of snapshot contents and object checksums.

    The local database is consulted when creating a snapshot to determine what
    data can be re-used from old snapshots.  Segment cleaning is performed by
    manipulating the data in the local database; the local database also
    includes enough data to guide the segment cleaning process.
    """

    def __init__(self, path, dbname="localdb.sqlite"):
        self.db_connection = sqlite3.connect(
            os.path.join(path, dbname),
            detect_types=sqlite3.PARSE_COLNAMES)

    @staticmethod
    def _get_id(item):
        """Fetch the id of a database object.

        If the input is an integer return it directly, otherwise try to fetch
        the .id field on it.
        """
        if isinstance(item, int):
            return item
        else:
            return item.id

    # Low-level database access.  Use these methods when there isn't a
    # higher-level interface available.  Exception: do, however, remember to
    # use the commit() method after making changes to make sure they are
    # actually saved, even when going through higher-level interfaces.
    def commit(self):
        "Commit any pending changes to the local database."
        self.db_connection.commit()

    def rollback(self):
        "Roll back any pending changes to the local database."
        self.db_connection.rollback()

    def cursor(self):
        "Return a DB-API cursor for directly accessing the local database."
        return self.db_connection.cursor()

    def get_snapshots(self):
        """Returns information about all snapshots in the local database.

        The returned value is a dictionary mapping scheme names to lists of
        snapshots for that scheme.  Each list entry is a SnapshotInfo instance.
        """
        cur = self.cursor()
        cur.execute("""select snapshotid, scheme, name,
                              datetime(timestamp) as "timestamp [timestamp]"
                       from snapshots order by scheme, name""")
        snapshots = {}
        for row in cur.fetchall():
            info = SnapshotInfo(*row)
            snapshots.setdefault(info.scheme, []).append(info)
        return snapshots

    def delete_snapshot(self, snapshot):
        """Remove the specified snapshot from the database.

        Returns a boolean indicating whether the snapshot was deleted.

        Warning: This does not garbage collect all dependent data in the
        database, so it must be followed by a call to garbage_collect() to make
        the database consistent.
        """
        cur = self.cursor()
        cur.execute("delete from snapshots where snapshotid = ?",
                    (self._get_id(snapshot),))
        return cur.rowcount > 0

    def garbage_collect(self):
        """Garbage-collect unreachable segment and object data.

        Remove all segments and checksums which is not reachable from the
        current set of snapshots stored in the local database.
        """
        cur = self.cursor()

        # Delete entries in the segment_utilization table which are for
        # non-existent snapshots.
        cur.execute("""delete from segment_utilization
                       where snapshotid not in
                           (select snapshotid from snapshots)""")

        # Delete segments not referenced by any current snapshots.
        cur.execute("""delete from segments where segmentid not in
                           (select segmentid from segment_utilization)""")

        # Delete dangling objects in the block_index table.
        cur.execute("""delete from block_index
                       where segmentid not in
                           (select segmentid from segments)""")

        # Remove sub-block signatures for deleted objects.
        cur.execute("""delete from subblock_signatures
                       where blockid not in
                           (select blockid from block_index)""")

    def get_segment_info(self):
        """Retrieve statistics about segments for cleaning decisions."""
        cur = self.cursor()
        cur.execute("""select segmentid, segment,
                              datetime(timestamp) as "timestamp [timestamp]",
                              data_size, disk_size, type
                       from segments""")
        return dict((x[0], SegmentInfo(*x)) for x in cur.fetchall())

    def get_segment_utilizations(self, snapshots):
        """Computes estimates for the data referenced in each segment.

        Computes a lower bound of the amount of data that is referenced in
        segments by the specified set of snapshots.
        """
        cur = self.cursor()
        segment_info = self.get_segment_info()
        snapshots = [self._get_id(s) for s in snapshots]

        query = """select segmentid, max(bytes_referenced)
                   from segment_utilization where snapshotid in (%s)
                   group by segmentid""" % (",".join(["?"] * len(snapshots)))
        segments = {}
        for row in cur.execute(query, snapshots):
            info = segment_info[row[0]]
            segments[row[0]] = SegmentStatistics(
                bytes_referenced=row[1],
                utilization=row[1]/info.data_size,
                **info._asdict())
        return segments

    def mark_segment_expired(self, segment):
        """Mark a segment for cleaning in the local database.

        The segment parameter should be either a SegmentInfo object or an
        integer segment id.  Objects in the given segment will be marked as
        expired, which means that any future snapshots that would re-use those
        objects will instead write out a new copy of the object, and thus no
        future snapshots will depend upon the given segment.
        """
        cur = self.cursor()
        cur.execute("update block_index set expired = 0 where segmentid = ?",
                    (self._get_id(segment),))

def run_cleaner(database):
    # Find the most recent snapshot for each backup scheme, then delete all
    # older snapshots from the database.
    kept_snapshots = []
    for snapshots in database.get_snapshots().values():
        snapshots = sorted(snapshots, key=lambda s: s.id)
        kept_snapshots.append(snapshots[-1])
        for s in snapshots[:-1]:
            print("Deleting snapshot", s)
            database.delete_snapshot(s)
    print("Keeping snapshots:", kept_snapshots)
    database.garbage_collect()

    # TODO: Look at adding more complex policies later, perhaps under user
    # control.  Cleaning policies to consider (which can be combined):
    #   - clean older than specified age
    #   - do not clean younger than specified age
    #   - clean below utilization threshold
    #   - do not allow data from previously-unreferenced segments
    #   - gradual: clean a fraction of segments which match a rule
    #   - minimum segment size (disk or data)
    #   - benefit calculation?

    for segment in database.get_segment_utilizations(kept_snapshots).values():
        if segment.utilization < 0.4:
            print("Clean segment:", segment)
            database.mark_segment_expired(segment)

    database.commit()

if __name__ == "__main__":
    # Demo usage: runs a cleaner on the database located in the current
    # directory.  This should be removed later when cleaning is properly hooked
    # up to the main tool.
    run_cleaner(Database("."))
