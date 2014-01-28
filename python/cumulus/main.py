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

"""The Python-based Cumulus script.

This implements maintenance functions and is a wrapper around the C++
cumulus-backup program.
"""

import datetime
import re
import sys

import cumulus
from cumulus import cmd_util
from cumulus import config

class FakeOptions:
    pass

def prune_backups(backup_config, scheme):
    store = cumulus.LowlevelDataStore(backup_config.get_global("dest"))
    snapshot_re = re.compile(r"^(.*)-(.*)$")
    retention = backup_config.get_retention_for_scheme(scheme)
    expired_snapshots = []
    for snapshot in sorted(store.list_snapshots()):
        m = snapshot_re.match(snapshot)
        if m.group(1) != scheme: continue
        timestamp = m.group(2)
        keep = retention.consider_snapshot(timestamp)
        if not keep:
            expired_snapshots.append(snapshot)
    # The most recent snapshot is never removed.
    if expired_snapshots: expired_snapshots.pop()
    print(expired_snapshots)

    # TODO: Clean up the expiration part...
    for snapshot in expired_snapshots:
        store.store.delete("snapshot", "snapshot-%s.lbs" % snapshot)

    print("Collecting garbage...")
    options = FakeOptions()
    options.store = backup_config.get_global("dest")
    options.dry_run = False
    cmd_util.options = options
    cmd_util.cmd_garbage_collect([])

def prune_localdb(backup_config, scheme, next_snapshot=None):
    """Clean old snapshots out of the local database.

    Clear old snapshots out of the local database, possibly in preparation for
    running a new backup.  One snapshot of each configured retention period is
    kept (i.e., one weekly and one daily), and the most recent snapshot is
    always retained.  If next_snapshot is not None, it should be the timestamp
    when (approximately) the next snapshot will be taken; if that snapshot
    would be a daily, weekly, etc. snapshot, then it may result in the previous
    snapshot of the same duration being evicted from the local database.

    Note that in this sense, "evict" merely refers to tracking the snapshots in
    the local database; this function does not delete backups from the backup
    storage.
    """
    # Fetch the list of existing snapshots in the local database.  Pruning only
    # makes sense if there are more than one snapshots present.
    db = cumulus.LocalDatabase(backup_config.get_global("localdb"))
    snapshots = sorted(db.list_snapshots(scheme))
    if len(snapshots) <= 1:
        return

    # Classify the snapshots (daily, weekly, etc.) and keep the most recent one
    # of each category.  Also ensure that the most recent snapshot is retained.
    retention = backup_config.get_retention_for_scheme(scheme)
    for snapshot in snapshots:
        retention.consider_snapshot(snapshot)
    if next_snapshot is not None:
        retention.consider_snapshot(next_snapshot)
    retained = set(retention.last_snapshots().values())
    retained.add(snapshots[-1])
    print(retention.last_snapshots())
    print(retained)
    for s in snapshots:
        print(s, s in retained)

    evicted = [s for s in snapshots if s not in retained]
    for s in evicted:
        db.delete_snapshot(scheme, s)
    db.garbage_collect()
    db.commit()

def main(argv):
    backup_config = config.CumulusConfig(argv[1])
    for scheme in backup_config.backup_schemes():
        print(scheme)
        #prune_backups(backup_config, scheme)
        prune_localdb(backup_config, scheme, datetime.datetime.utcnow())
        #prune_localdb(backup_config, scheme, datetime.datetime(2013, 1, 1))

if __name__ == "__main__":
    main(sys.argv)
