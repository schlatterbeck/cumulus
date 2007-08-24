#!/usr/bin/python
#
# Utility for managing LBS archives.

import sys
from optparse import OptionParser
import lbs

parser = OptionParser(usage="%prog [option]... command [arg]...")
parser.add_option("-v", action="store_true", dest="verbose", default=False,
                  help="increase verbosity")
parser.add_option("--localdb", dest="localdb",
                  help="specify path to local database")
(options, args) = parser.parse_args(sys.argv[1:])

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

if len(args) == 0:
    parser.print_usage()
    sys.exit(1)
cmd = args[0]
args = args[1:]
if cmd == 'clean':
    cmd_clean()
else:
    print "Unknown command:", cmd
    parser.print_usage()
    sys.exit(1)
