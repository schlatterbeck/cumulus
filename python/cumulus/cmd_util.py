# Cumulus: Efficient Filesystem Backup to the Cloud
# Copyright (C) 2006-2009, 2012 The Cumulus Developers
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

"""Implementation of the Cumulus command-line utility program."""

import getpass, os, stat, sys, time
from optparse import OptionParser

import cumulus

# We support up to "Cumulus Snapshot v0.11" formats, but are also limited by
# the cumulus module.
FORMAT_VERSION = min(cumulus.FORMAT_VERSION, (0, 11))

def check_version(format):
    ver = cumulus.parse_metadata_version(format)
    if ver > FORMAT_VERSION:
        raise RuntimeError("Unsupported Cumulus format: " + format)

# Read a passphrase from the user and store it in the LBS_GPG_PASSPHRASE
# environment variable.
def get_passphrase():
    ENV_KEY = 'LBS_GPG_PASSPHRASE'
    if ENV_KEY not in os.environ:
        os.environ[ENV_KEY] = getpass.getpass()

def cmd_prune_db(args):
    """ Delete old snapshots from the local database, though do not
        actually schedule any segment cleaning.
        Syntax: $0 --localdb=LOCALDB prune-db
    """
    db = cumulus.LocalDatabase(options.localdb)

    # Delete old snapshots from the local database.
    #db.garbage_collect()
    #db.commit()

def cmd_clean(args, clean_threshold=7.0):
    """ Run the segment cleaner.
        Syntax: $0 --localdb=LOCALDB clean
    """
    db = cumulus.LocalDatabase(options.localdb)

    # Delete old snapshots from the local database.
    intent = float(options.intent)
    for s in db.list_schemes():
        db.prune_old_snapshots(s, intent)

    # Expire segments which are poorly-utilized.
    for s in db.get_segment_cleaning_list():
        if s.cleaning_benefit > clean_threshold:
            print("Cleaning segment %d (benefit %.2f)" % (s.id,
                                                          s.cleaning_benefit))
            db.mark_segment_expired(s)
        else:
            break
    db.balance_expired_objects()
    db.commit()

def cmd_list_snapshots(args):
    """ List snapshots stored.
        Syntax: $0 --data=DATADIR list-snapshots
    """
    store = cumulus.CumulusStore(options.store)
    for s in sorted(store.list_snapshots()): print(s)

def cmd_list_snapshot_sizes(args):
    """ List size of data needed for each snapshot.
        Syntax: $0 --data=DATADIR list-snapshot-sizes
    """
    store = cumulus.CumulusStore(options.store)
    backend = store.backend
    backend.prefetch_generic()
    previous = set()
    size = 0
    def get_size(segment):
        return backend.stat_generic(segment + ".tar", "segments")["size"]
    for s in sorted(store.list_snapshots()):
        d = cumulus.parse_full(store.load_snapshot(s))
        check_version(d['Format'])

        segments = set(d['Segments'].split())
        (added, removed, addcount, remcount) = (0, 0, 0, 0)
        for seg in segments.difference(previous):
            added += get_size(seg)
            addcount += 1
        for seg in previous.difference(segments):
            removed += get_size(seg)
            remcount += 1
        size += added - removed
        previous = segments
        print("%s: %.3f +%.3f -%.3f (+%d/-%d segments)" % (s, size / 1024.0**2, added / 1024.0**2, removed / 1024.0**2, addcount, remcount))

def cmd_garbage_collect(args):
    """ Search for any files which are not needed by any current
        snapshots and offer to delete them.
        Syntax: $0 --store=DATADIR gc
    """
    store = cumulus.CumulusStore(options.store)
    backend = store.backend
    referenced = set()
    for s in store.list_snapshots():
        d = cumulus.parse_full(store.load_snapshot(s))
        check_version(d['Format'])
        referenced.add(s)
        referenced.update(d['Segments'].split())

    print(referenced)

    to_delete = []
    to_preserve = []
    for filetype in cumulus.SEARCH_PATHS:
        for (name, path) in store.backend.list_generic(filetype):
            if name in referenced:
                to_preserve.append(path)
            else:
                to_delete.append(path)

    print(to_preserve)
    print(to_delete)

    raw_backend = backend.raw_backend
    for f in to_delete:
        print("Delete:", f)
        if not options.dry_run:
            raw_backend.delete(f)
cmd_gc = cmd_garbage_collect

def cmd_read_snapshots(snapshots):
    """ Read a snapshot file
    """
    get_passphrase()
    store = cumulus.CumulusStore(options.store)
    for s in snapshots:
        d = cumulus.parse_full(store.load_snapshot(s))
        check_version(d['Format'])
        print(d)
        print(d['Segments'].split())
    store.cleanup()

def cmd_read_metadata(args):
    """ Produce a flattened metadata dump from a snapshot
    """
    snapshot = args [0]
    get_passphrase()
    store = cumulus.CumulusStore(options.store)
    d = cumulus.parse_full(store.load_snapshot(snapshot))
    check_version(d['Format'])
    metadata = cumulus.read_metadata(store, d['Root'])
    blank = True
    for l in metadata:
        if l == '\n':
            if blank: continue
            blank = True
        else:
            blank = False
        sys.stdout.write(l)
    store.cleanup()

def cmd_verify_snapshots(snapshots):
    """ Verify snapshot integrity
    """
    get_passphrase()
    store = cumulus.CumulusStore(options.store)
    for s in snapshots:
        cumulus.accessed_segments.clear()
        print("#### Snapshot", s)
        d = cumulus.parse_full(store.load_snapshot(s))
        check_version(d['Format'])
        print("## Root:", d['Root'])
        metadata = cumulus.iterate_metadata(store, d['Root'])
        for m in metadata:
            if m.fields['type'] not in ('-', 'f'): continue
            print("%s [%d bytes]" % (m.fields['name'], int(m.fields['size'])))
            verifier = cumulus.ChecksumVerifier(m.fields['checksum'])
            size = 0
            for block in m.data():
                data = store.get(block)
                verifier.update(data)
                size += len(data)
            if int(m.fields['size']) != size:
                raise ValueError("File size does not match!")
            if not verifier.valid():
                raise ValueError("Bad checksum found")

        # Verify that the list of segments included with the snapshot was
        # actually accurate: covered all segments that were really read, and
        # doesn't contain duplicates.
        listed_segments = set(d['Segments'].split())
        if cumulus.accessed_segments - listed_segments:
            print("Error: Some segments not listed in descriptor!")
            print(sorted(list(cumulus.accessed_segments - listed_segments)))
        if listed_segments - cumulus.accessed_segments :
            print("Warning: Extra unused segments listed in descriptor!")
            print(sorted(list(listed_segments - cumulus.accessed_segments)))
    store.cleanup()

def cmd_restore_snapshot(args):
    """ Restore a snapshot, or some subset of files from it
    """
    get_passphrase()
    store = cumulus.CumulusStore(options.store)
    snapshot = cumulus.parse_full(store.load_snapshot(args[0]))
    check_version(snapshot['Format'])
    destdir = args[1]
    paths = args[2:]

    def matchpath(path):
        "Return true if the specified path should be included in the restore."

        # No specification of what to restore => restore everything
        if len(paths) == 0: return True

        for p in paths:
            if path == p: return True
            if path.startswith(p + "/"): return True
        return False

    def warn(m, msg):
        print("Warning: %s: %s" % (m.items.name, msg))

    # Phase 1: Read the complete metadata log and create directory structure.
    metadata_items = []
    metadata_paths = {}
    metadata_segments = {}
    for m in cumulus.iterate_metadata(store, snapshot['Root']):
        pathname = os.path.normpath(m.items.name)
        while os.path.isabs(pathname):
            pathname = pathname[1:]
        if not matchpath(pathname): continue

        destpath = os.path.join(destdir, pathname)
        if m.items.type == 'd':
            path = destpath
        else:
            (path, filename) = os.path.split(destpath)

        metadata_items.append((pathname, m))
        if m.items.type in ('-', 'f'):
            metadata_paths[pathname] = m
            for block in m.data():
                (segment, object, checksum, slice) \
                    = cumulus.CumulusStore.parse_ref(block)
                if segment not in metadata_segments:
                    metadata_segments[segment] = set()
                metadata_segments[segment].add(pathname)

        try:
            if not os.path.isdir(path):
                print("mkdir:", path)
                os.makedirs(path)
        except Exception as e:
            warn(m, "Error creating directory structure: %s" % (e,))
            continue

    # Phase 2: Restore files, ordered by how data is stored in segments.
    def restore_file(pathname, m):
        assert m.items.type in ('-', 'f')
        print("extract:", pathname)
        destpath = os.path.join(destdir, pathname)

        file = open(destpath, 'wb')
        verifier = cumulus.ChecksumVerifier(m.items.checksum)
        size = 0
        for block in m.data():
            data = store.get(block)
            verifier.update(data)
            size += len(data)
            file.write(data)
        file.close()
        if int(m.fields['size']) != size:
            raise ValueError("File size does not match!")
        if not verifier.valid():
            raise ValueError("Bad checksum found")

    while metadata_segments:
        (segment, items) = metadata_segments.popitem()
        print("+ Segment", segment)
        for pathname in sorted(items):
            if pathname in metadata_paths:
                restore_file(pathname, metadata_paths[pathname])
                del metadata_paths[pathname]

    print("+ Remaining files")
    while metadata_paths:
        (pathname, m) = metadata_paths.popitem()
        restore_file(pathname, m)

    # Phase 3: Restore special files (symlinks, devices).
    # Phase 4: Restore directory permissions and modification times.
    for (pathname, m) in reversed(metadata_items):
        print("permissions:", pathname)
        destpath = os.path.join(destdir, pathname)
        (path, filename) = os.path.split(destpath)

        # TODO: Check for ../../../paths that might attempt to write outside
        # the destination directory.  Maybe also check attempts to follow
        # symlinks pointing outside?

        try:
            if m.items.type in ('-', 'f', 'd'):
                pass
            elif m.items.type == 'l':
                try:
                    target = m.items.target
                except:
                    # Old (v0.2 format) name for 'target'
                    target = m.items.contents
                os.symlink(target, destpath)
            elif m.items.type == 'p':
                os.mkfifo(destpath)
            elif m.items.type in ('c', 'b'):
                if m.items.type == 'c':
                    mode = 0o600 | stat.S_IFCHR
                else:
                    mode = 0o600 | stat.S_IFBLK
                os.mknod(destpath, mode, os.makedev(*m.items.device))
            elif m.items.type == 's':
                pass        # TODO: Implement
            else:
                warn(m, "Unknown type code: " + m.items.type)
                continue

        except Exception as e:
            warn(m, "Error restoring: %s" % (e,))
            continue

        try:
            uid = m.items.user[0]
            gid = m.items.group[0]
            os.lchown(destpath, uid, gid)
        except Exception as e:
            warn(m, "Error restoring file ownership: %s" % (e,))

        if m.items.type == 'l':
            continue

        try:
            os.chmod(destpath, m.items.mode)
        except Exception as e:
            warn(m, "Error restoring file permissions: %s" % (e,))

        try:
            os.utime(destpath, (time.time(), m.items.mtime))
        except Exception as e:
            warn(m, "Error restoring file timestamps: %s" % (e,))

    store.cleanup()

def main(argv):
    usage = ["%prog [option]... command [arg]...", "", "Commands:"]
    cmd = method = None
    for cmd, method in globals().items():
        if cmd.startswith ('cmd_'):
            usage.append(cmd[4:].replace('_', '-') + ':' + method.__doc__)
    parser = OptionParser(usage="\n".join(usage))
    parser.add_option("-v", action="store_true", dest="verbose", default=False,
                      help="increase verbosity")
    parser.add_option("-n", action="store_true", dest="dry_run", default=False,
                      help="dry run")
    parser.add_option("--store", dest="store",
                      help="specify path to backup data store")
    parser.add_option("--localdb", dest="localdb",
                      help="specify path to local database")
    parser.add_option("--intent", dest="intent", default=1.0,
                      help="give expected next snapshot type when cleaning")
    global options
    (options, args) = parser.parse_args(argv[1:])

    if len(args) == 0:
        parser.print_usage()
        sys.exit(1)
    cmd = args[0]
    args = args[1:]
    method = globals().get('cmd_' + cmd.replace('-', '_'))
    if method:
        method (args)
    else:
        print("Unknown command:", cmd)
        parser.print_usage()
        sys.exit(1)
