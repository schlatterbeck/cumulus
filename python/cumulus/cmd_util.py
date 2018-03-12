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

from __future__ import division, print_function, unicode_literals

import getpass, os, stat, sys, time
from datetime import datetime, timedelta
from optparse import OptionParser
from time     import strftime, localtime

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

def cmd_clean(args, clean_threshold=7.0):
    """ Delete old snapshots from the local database, then
        run the segment cleaner.
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

def cmd_expire_local_segments(args):
    """ Remove local segments so that they are not used during next
        backup -- useful if some segments have disappeared on remote
        storage for whatever reason and we want new backups to be
        consistent.
        Syntax: $0 --localdb=LOCALDB expire-local-segments
    """
    db = lbs.LocalDatabase(options.localdb)
    csr = db.cursor()
    segs = ','.join ('"%s"' % a for a in args)
    csr.execute('''select segmentid,segment from segments
                   where segment in (%s)''' % segs)

    segments = []
    # first make a copy before re-using cursor object
    for id, name in csr:
        segments.append ((id, name))
    for id, name in segments:
        print("Expiring segment %s (%d)" % (name, id))
        db.mark_segment_expired(id)
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
        Syntax: $0 --data=DATADIR [--latest <n>] list-snapshot-sizes [snapshots]
    """
    store = cumulus.CumulusStore(options.store)
    backend = store.backend
    backend.prefetch_generic()
    previous = set()
    size = 0
    def get_size(segment):
        return backend.stat_generic(segment + ".tar", "segments")["size"]
    snapshots = args
    if not snapshots:
        snapshots=store.list_snapshots()
        if options.latest:
            snapshots = (list (sorted (snapshots))) [-options.latest:]
    for s in sorted(snapshots):
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
        snapshots and delete them unless dry_run (-n option) is set.
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

def format_ls (meta):
    """ Pretty-print meta object similar to ls -1 output
    """
    print(meta.items.name)

def format_ls_l (meta):
    """ Pretty-print meta object similar to ls -l output
    """
    type = meta.items.type
    if type == 'f' :
        type = '-'
    modes = 'xwr'
    binmode = meta.items.mode
    mode = []
    for i in range (9) :
        if binmode & (1 << i) :
            mode.append (modes [i % 3])
        else :
            mode.append ('-')
    mode = ''.join (reversed (mode))
    user  = meta.items.user  [1]
    group = meta.items.group [1]
    size = ''
    if 'size' in meta.fields :
        size = meta.items.size
    date = strftime ('%Y-%m-%d %H:%M', localtime (meta.items.mtime))
    name = meta.items.name
    print("%s%s %11s %11s %10s %s %s"
         % (type, mode, user, group, size, date, name)
         )

def ls(args, prettyprinter):
    """ Produce a flattened metadata dump from a snapshot
        Listing is in the format given by prettyprinter function
    """
    snapshot = args [0]
    get_passphrase()
    lowlevel = lbs.LowlevelDataStore(options.store)
    store = lbs.ObjectStore(lowlevel)
    d = lbs.parse_full(store.load_snapshot(snapshot))
    check_version(d['Format'])
    for m in lbs.iterate_metadata(store, d['Root']) :
        prettyprinter(m)
    store.cleanup()

def cmd_ls(args):
    """ Produce a flattened metadata dump from a snapshot
        Format is similar to ls output (only one column)
    """
    ls(args, format_ls)

def cmd_lsl(args):
    """ Produce a flattened metadata dump from a snapshot
        Format is similar to ls -l output
    """
    ls(args, format_ls_l)

cmd_dir = cmd_lsl

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
        Syntax: $0 --store=DATADIR verify-snapshots s1 ...
        You need to specify at least one snapshot
        Note: This is costly, it will read the whole snapshot from
        remote storage
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
    """ Restore a snapshot, or some subset of files from it to directory dest
        Syntax: $0 --store=DATADIR restore-snapshot snapshot dest [files]
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

def cmd_remove_old_snapshots(args):
    """ Remove snapshots:
        - daily snapshots (all except weekly) older than a week, this is
          done separately for each weekday (i.e. we keep latest Mon,
          Tue, Wed, Thu, Fri, Sun backups)
        - weekly for 1st, 2nd, 3rd and 5th week in month
        - monthly (4th week in month) are never removed
        Default convention is that the weekly snapshots are done on
        Friday with an offset of -5 hours (we do Friday backups early on
        Saturday morning, subtracting 5 hours from the backup timestamp
        will give us Friday). You may alter this by specifying e.g.
        weekday=Sat for weekly backups on Saturday as an argument.
        You may also change the offset by specifying e.g., offset=0

        Note: If you alter the weekday you should choose your weekly
        backup day and stick to it -- otherwise if you change it, you
        will remove old monthly backups when first removing old
        snapshots for "Fri" and then for "Sat" -- the net result is that
        after this you only have backups for the last week left!

        After running this command you need to do a garbage-collect run.
        We currently only remove the top-level .lbs file.

        Also note that this command runs the clean command first, so
        that snapshot in the local database are expired. So you need to
        specify both, the local db and the remote storage.

        WARNING: THIS REMOVES SNAPSHOTS ON THE REMOTE STORAGE!
    """
    if 'clean_threshold' in args:
        del args ['clean_threshold']
    cmd_clean (args)
    weekdays = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    opttypes = {'weekday': str, 'offset': int}
    opts = {'weekday' : 'Fri', 'offset' : -5}
    for a in args:
        if '=' not in a:
            print("Expected parameter=value: %s" % a, file=sys.stderr)
            return
        k, v = a.split('=', 1)
        if k not in opttypes:
            print("Unknown parameter: %s" % k, file=sys.stderr)
            return
        opts [k] = opttypes [k](v)
    store = cumulus.CumulusStore(options.store)
    td = timedelta (hours = opts['offset'])
    datebyname = {}
    prettybyname = {}
    latest_daily = {}
    latest_weekly = {}
    to_remove = {}
    for snapname in lowlevel.list_snapshots():
        scheme, timestamp = snapname.rsplit('-', 1)
        dt = datetime.strptime(timestamp, "%Y%m%dT%H%M%S")
        dt += td
        weekday = dt.strftime('%a')
        weekno = (int (dt.strftime('%d')) - 1) / 7
        datebyname[snapname] = dt
        prettybyname[snapname] = dt.strftime('%b')
        if weekday != opts['weekday']:
            prettybyname[snapname] = dt.strftime('%a')
            if (scheme, weekday) in latest_daily:
                olddt = datebyname[latest_daily[scheme, weekday]]
                if dt > olddt:
                    to_remove [latest_daily[scheme, weekday]] = True
                    latest_daily[scheme, weekday] = snapname
                else:
                    to_remove [snapname] = True
            else:
                latest_daily[(scheme, weekday)] = snapname
        elif weekno != 4 - 1:
            prettybyname[snapname] = 'W %s' % (weekno + 1)
            if (scheme, weekno) in latest_weekly:
                olddt = datebyname[latest_weekly[scheme, weekno]]
                if dt > olddt:
                    to_remove [latest_weekly[scheme, weekno]] = True
                    latest_weekly[scheme, weekno] = snapname
                else:
                    to_remove [snapname] = True
            else:
                latest_weekly[scheme, weekno] = snapname

    t = 'snapshots'
    r = cumulus.store.type_patterns[t]
    for f in sorted(lowlevel.store.list(t)):
        m = r.match(f)
        if m:
            snapname = m.group(1)
            print(snapname, prettybyname [snapname], end=' ')
            if snapname in to_remove:
                if options.dry_run:
                    print("not deleted (dry-run)")
                else:
                    lowlevel.store.delete(t, f)
                    print("deleted")
            else:
                print("kept")

def main(argv):
    usage = ["%prog [option]... command [arg]...", "", "Commands:"]
    cmd = method = None
    for cmd, method in sorted(globals().items()):
        if cmd.startswith ('cmd_'):
            cmd = cmd[4:].replace('_', '-')
            doc = method.__doc__.replace ('$0', os.path.basename(sys.argv[0]))
            usage.append(':'.join((cmd, doc)))
    parser = OptionParser(usage="\n".join(usage))
    parser.add_option("-v", action="store_true", dest="verbose", default=False,
                      help="increase verbosity")
    parser.add_option("-n", "--dry-run", action="store_true", dest="dry_run",
                      default=False, help="dry run")
    parser.add_option("--latest", dest="latest", type="int", default=0,
                      help="List latest n snapshots for list-snapshot-sizes")
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
