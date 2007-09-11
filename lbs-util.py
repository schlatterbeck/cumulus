#!/usr/bin/python
#
# Utility for managing LBS archives.

import getpass, os, sys
from optparse import OptionParser
import lbs

parser = OptionParser(usage="%prog [option]... command [arg]...")
parser.add_option("-v", action="store_true", dest="verbose", default=False,
                  help="increase verbosity")
parser.add_option("--store", dest="store",
                  help="specify path to backup data store")
parser.add_option("--localdb", dest="localdb",
                  help="specify path to local database")
(options, args) = parser.parse_args(sys.argv[1:])

# Read a passphrase from the user and store it in the LBS_GPG_PASSPHRASE
# environment variable.
def get_passphrase():
    ENV_KEY = 'LBS_GPG_PASSPHRASE'
    if not os.environ.has_key(ENV_KEY):
        os.environ[ENV_KEY] = getpass.getpass()

# Run the segment cleaner.
# Syntax: $0 --localdb=LOCALDB clean
def cmd_clean(clean_threshold=7.0):
    db = lbs.LocalDatabase(options.localdb)

    # Delete old snapshots from the local database.
    db.garbage_collect()

    # Expire segments which are poorly-utilized.
    for s in db.get_segment_cleaning_list():
        if s.cleaning_benefit > clean_threshold:
            print "Cleaning segment %d (benefit %.2f)" % (s.id,
                                                          s.cleaning_benefit)
            db.mark_segment_expired(s)
        else:
            break
    db.balance_expired_objects()
    db.commit()

# List snapshots stored.
# Syntax: $0 --data=DATADIR list-snapshots
def cmd_list_snapshots():
    store = lbs.LowlevelDataStore(options.store)
    for s in sorted(store.list_snapshots()):
        print s

# List size of data needed for each snapshot.
# Syntax: $0 --data=DATADIR list-snapshot-sizes
def cmd_list_snapshot_sizes():
    lowlevel = lbs.LowlevelDataStore(options.store)
    store = lbs.ObjectStore(lowlevel)
    previous = set()
    for s in sorted(lowlevel.list_snapshots()):
        d = lbs.parse_full(store.load_snapshot(s))
        segments = d['Segments'].split()
        (size, added, removed) = (0, 0, 0)
        for seg in segments:
            segsize = lowlevel.lowlevel_stat(seg + ".tar.gpg")['size']
            size += segsize
            if seg not in previous: added += segsize
        for seg in previous:
            if seg not in segments:
                removed += lowlevel.lowlevel_stat(seg + ".tar.gpg")['size']
        previous = set(segments)
        print "%s: %.3f +%.3f -%.3f" % (s, size / 1024.0**2, added / 1024.0**2, removed / 1024.0**2)

# Build checksum list for objects in the given segments
def cmd_object_checksums(segments):
    lowlevel = lbs.LowlevelDataStore(options.store)
    store = lbs.ObjectStore(lowlevel)
    for s in segments:
        for (o, data) in store.load_segment(s):
            csum = lbs.ChecksumCreator().update(data).compute()
            print "%s/%s:%d:%s" % (s, o, len(data), csum)
    store.cleanup()

# Read a snapshot file
def cmd_read_snapshots(snapshots):
    get_passphrase()
    lowlevel = lbs.LowlevelDataStore(options.store)
    store = lbs.ObjectStore(lowlevel)
    for s in snapshots:
        d = lbs.parse_full(store.load_snapshot(s))
        print d
        print d['Segments'].split()
    store.cleanup()

# Verify snapshot integrity
def cmd_verify_snapshots(snapshots):
    get_passphrase()
    lowlevel = lbs.LowlevelDataStore(options.store)
    store = lbs.ObjectStore(lowlevel)
    for s in snapshots:
        print "#### Snapshot", s
        d = lbs.parse_full(store.load_snapshot(s))
        print "## Root:", d['Root']
        metadata = lbs.iterate_metadata(store, d['Root'])
        for m in metadata:
            if m.fields['type'] != '-': continue
            print "%s [%d bytes]" % (m.fields['name'], int(m.fields['size']))
            verifier = lbs.ChecksumVerifier(m.fields['checksum'])
            size = 0
            for block in m.data():
                data = store.get(block)
                verifier.update(data)
                size += len(data)
            if int(m.fields['size']) != size:
                raise ValueError("File size does not match!")
            if not verifier.valid():
                raise ValueError("Bad checksum found")
    store.cleanup()

if len(args) == 0:
    parser.print_usage()
    sys.exit(1)
cmd = args[0]
args = args[1:]
if cmd == 'clean':
    cmd_clean()
elif cmd == 'list-snapshots':
    cmd_list_snapshots()
elif cmd == 'object-sums':
    cmd_object_checksums(args)
elif cmd == 'read-snapshots':
    cmd_read_snapshots(args)
elif cmd == 'list-snapshot-sizes':
    cmd_list_snapshot_sizes()
elif cmd == 'verify-snapshots':
    cmd_verify_snapshots(args)
else:
    print "Unknown command:", cmd
    parser.print_usage()
    sys.exit(1)
