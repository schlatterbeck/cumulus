"""High-level interface for working with LBS archives.

This module provides an easy interface for reading from and manipulating
various parts of an LBS archive:
  - listing the snapshots and segments present
  - reading segment contents
  - parsing snapshot descriptors and snapshot metadata logs
  - reading and maintaining the local object database
"""

from __future__ import division
import os, re, sha, tarfile, tempfile, thread
from pysqlite2 import dbapi2 as sqlite3

# The largest supported snapshot format that can be understood.
FORMAT_VERSION = (0, 6)         # LBS Snapshot v0.6

# Maximum number of nested indirect references allowed in a snapshot.
MAX_RECURSION_DEPTH = 3

# All segments which have been accessed this session.
accessed_segments = set()

class Struct:
    """A class which merely acts as a data container.

    Instances of this class (or its subclasses) are merely used to store data
    in various attributes.  No methods are provided.
    """

    def __repr__(self):
        return "<%s %s>" % (self.__class__, self.__dict__)

CHECKSUM_ALGORITHMS = {
    'sha1': sha.new
}

class ChecksumCreator:
    """Compute an LBS checksum for provided data.

    The algorithm used is selectable, but currently defaults to sha1.
    """

    def __init__(self, algorithm='sha1'):
        self.algorithm = algorithm
        self.hash = CHECKSUM_ALGORITHMS[algorithm]()

    def update(self, data):
        self.hash.update(data)
        return self

    def compute(self):
        return "%s=%s" % (self.algorithm, self.hash.hexdigest())

class ChecksumVerifier:
    """Verify whether a checksum from a snapshot matches the supplied data."""

    def __init__(self, checksumstr):
        """Create an object to check the supplied checksum."""

        (algo, checksum) = checksumstr.split("=", 1)
        self.checksum = checksum
        self.hash = CHECKSUM_ALGORITHMS[algo]()

    def update(self, data):
        self.hash.update(data)

    def valid(self):
        """Return a boolean indicating whether the checksum matches."""

        result = self.hash.hexdigest()
        return result == self.checksum

class LowlevelDataStore:
    """Access to the backup store containing segments and snapshot descriptors.

    Instances of this class are used to get direct filesystem-level access to
    the backup data.  To read a backup, a caller will ordinarily not care about
    direct access to backup segments, but will instead merely need to access
    objects from those segments.  The ObjectStore class provides a suitable
    wrapper around a DataStore to give this high-level access.
    """

    def __init__(self, path):
        self.path = path

    # Low-level filesystem access.  These methods could be overwritten to
    # provide access to remote data stores.
    def lowlevel_list(self):
        """Get a listing of files stored."""

        return os.listdir(self.path)

    def lowlevel_open(self, filename):
        """Return a file-like object for reading data from the given file."""

        return open(os.path.join(self.path, filename), 'rb')

    def lowlevel_stat(self, filename):
        """Return a dictionary of information about the given file.

        Currently, the only defined field is 'size', giving the size of the
        file in bytes.
        """

        stat = os.stat(os.path.join(self.path, filename))
        return {'size': stat.st_size}

    # Slightly higher-level list methods.
    def list_snapshots(self):
        for f in self.lowlevel_list():
            m = re.match(r"^snapshot-(.*)\.lbs$", f)
            if m:
                yield m.group(1)

    def list_segments(self):
        for f in self.lowlevel_list():
            m = re.match(r"^([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(\.\S+)?$", f)
            if m:
                yield m.group(1)

class ObjectStore:
    def __init__(self, data_store):
        self.store = data_store
        self.cachedir = None
        self.CACHE_SIZE = 16
        self.lru_list = []

    def get_cachedir(self):
        if self.cachedir is None:
            self.cachedir = tempfile.mkdtemp(".lbs")
        return self.cachedir

    def cleanup(self):
        if self.cachedir is not None:
            # TODO: Avoid use of system, make this safer
            os.system("rm -rf " + self.cachedir)
        self.cachedir = None

    @staticmethod
    def parse_ref(refstr):
        m = re.match(r"^zero\[(\d+)\]$", refstr)
        if m:
            return ("zero", None, None, (0, int(m.group(1))))

        m = re.match(r"^([-0-9a-f]+)\/([0-9a-f]+)(\(\S+\))?(\[((\d+)\+)?(\d+)\])?$", refstr)
        if not m: return

        segment = m.group(1)
        object = m.group(2)
        checksum = m.group(3)
        slice = m.group(4)

        if checksum is not None:
            checksum = checksum.lstrip("(").rstrip(")")

        if slice is not None:
            if m.group(5) is None:
                # Abbreviated slice
                slice = (0, int(m.group(7)))
            else:
                slice = (int(m.group(6)), int(m.group(7)))

        return (segment, object, checksum, slice)

    def get_segment(self, segment):
        accessed_segments.add(segment)
        raw = self.store.lowlevel_open(segment + ".tar.gpg")

        (input, output) = os.popen2("lbs-filter-gpg --decrypt")
        def copy_thread(src, dst):
            BLOCK_SIZE = 4096
            while True:
                block = src.read(BLOCK_SIZE)
                if len(block) == 0: break
                dst.write(block)
            dst.close()

        thread.start_new_thread(copy_thread, (raw, input))
        return output

    def load_segment(self, segment):
        seg = tarfile.open(segment, 'r|', self.get_segment(segment))
        for item in seg:
            data_obj = seg.extractfile(item)
            path = item.name.split('/')
            if len(path) == 2 and path[0] == segment:
                yield (path[1], data_obj.read())

    def load_snapshot(self, snapshot):
        file = self.store.lowlevel_open("snapshot-" + snapshot + ".lbs")
        return file.read().splitlines(True)

    def extract_segment(self, segment):
        segdir = os.path.join(self.get_cachedir(), segment)
        os.mkdir(segdir)
        for (object, data) in self.load_segment(segment):
            f = open(os.path.join(segdir, object), 'wb')
            f.write(data)
            f.close()

    def load_object(self, segment, object):
        accessed_segments.add(segment)
        path = os.path.join(self.get_cachedir(), segment, object)
        if not os.access(path, os.R_OK):
            self.extract_segment(segment)
        if segment in self.lru_list: self.lru_list.remove(segment)
        self.lru_list.append(segment)
        while len(self.lru_list) > self.CACHE_SIZE:
            os.system("rm -rf " + os.path.join(self.cachedir, self.lru_list[0]))
            self.lru_list = self.lru_list[1:]
        return open(path, 'rb').read()

    def get(self, refstr):
        """Fetch the given object and return it.

        The input should be an object reference, in string form.
        """

        (segment, object, checksum, slice) = self.parse_ref(refstr)

        if segment == "zero":
            return "\0" * slice[1]

        data = self.load_object(segment, object)

        if checksum is not None:
            verifier = ChecksumVerifier(checksum)
            verifier.update(data)
            if not verifier.valid():
                raise ValueError

        if slice is not None:
            (start, length) = slice
            data = data[start:start+length]
            if len(data) != length: raise IndexError

        return data

def parse(lines, terminate=None):
    """Generic parser for RFC822-style "Key: Value" data streams.

    This parser can be used to read metadata logs and snapshot root descriptor
    files.

    lines must be an iterable object which yields a sequence of lines of input.

    If terminate is specified, it is used as a predicate to determine when to
    stop reading input lines.
    """

    dict = {}
    last_key = None

    for l in lines:
        # Strip off a trailing newline, if present
        if len(l) > 0 and l[-1] == "\n":
            l = l[:-1]

        if terminate is not None and terminate(l):
            if len(dict) > 0: yield dict
            dict = {}
            last_key = None
            continue

        m = re.match(r"^(\w+):\s*(.*)$", l)
        if m:
            dict[m.group(1)] = m.group(2)
            last_key = m.group(1)
        elif len(l) > 0 and l[0].isspace() and last_key is not None:
            dict[last_key] += l
        else:
            last_key = None

    if len(dict) > 0: yield dict

def parse_full(lines):
    try:
        return parse(lines).next()
    except StopIteration:
        return {}

def parse_metadata_version(s):
    """Convert a string with the snapshot version format to a tuple."""

    m = re.match(r"^LBS Snapshot v(\d+(\.\d+)*)$", s)
    if m is None:
        return ()
    else:
        return tuple([int(d) for d in m.group(1).split(".")])

def read_metadata(object_store, root):
    """Iterate through all lines in the metadata log, following references."""

    # Stack for keeping track of recursion when following references to
    # portions of the log.  The last entry in the stack corresponds to the
    # object currently being parsed.  Each entry is a list of lines which have
    # been reversed, so that popping successive lines from the end of each list
    # will return lines of the metadata log in order.
    stack = []

    def follow_ref(refstr):
        if len(stack) >= MAX_RECURSION_DEPTH: raise OverflowError
        lines = object_store.get(refstr).splitlines(True)
        lines.reverse()
        stack.append(lines)

    follow_ref(root)

    while len(stack) > 0:
        top = stack[-1]
        if len(top) == 0:
            stack.pop()
            continue
        line = top.pop()

        # An indirect reference which we must follow?
        if len(line) > 0 and line[0] == '@':
            ref = line[1:]
            ref.strip()
            follow_ref(ref)
        else:
            yield line

class MetadataItem:
    """Metadata for a single file (or directory or...) from a snapshot."""

    # Functions for parsing various datatypes that can appear in a metadata log
    # item.
    @staticmethod
    def decode_int(s):
        """Decode an integer, expressed in decimal, octal, or hexadecimal."""
        if s.startswith("0x"):
            return int(s, 16)
        elif s.startswith("0"):
            return int(s, 8)
        else:
            return int(s, 10)

    @staticmethod
    def decode_str(s):
        """Decode a URI-encoded (%xx escapes) string."""
        def hex_decode(m): return chr(int(m.group(1), 16))
        return re.sub(r"%([0-9a-f]{2})", hex_decode, s)

    @staticmethod
    def raw_str(s):
        """An unecoded string."""
        return s

    @staticmethod
    def decode_user(s):
        """Decode a user/group to a tuple of uid/gid followed by name."""
        items = s.split()
        uid = MetadataItem.decode_int(items[0])
        name = None
        if len(items) > 1:
            if items[1].startswith("(") and items[1].endswith(")"):
                name = MetadataItem.decode_str(items[1][1:-1])
        return (uid, name)

    @staticmethod
    def decode_device(s):
        """Decode a device major/minor number."""
        (major, minor) = map(MetadataItem.decode_int, s.split("/"))
        return (major, minor)

    class Items: pass

    def __init__(self, fields, object_store):
        """Initialize from a dictionary of key/value pairs from metadata log."""

        self.fields = fields
        self.object_store = object_store
        self.keys = []
        self.items = self.Items()
        for (k, v) in fields.items():
            if k in self.field_types:
                decoder = self.field_types[k]
                setattr(self.items, k, decoder(v))
                self.keys.append(k)

    def data(self):
        """Return an iterator for the data blocks that make up a file."""

        # This traverses the list of blocks that make up a file, following
        # indirect references.  It is implemented in much the same way as
        # read_metadata, so see that function for details of the technique.

        objects = self.fields['data'].split()
        objects.reverse()
        stack = [objects]

        def follow_ref(refstr):
            if len(stack) >= MAX_RECURSION_DEPTH: raise OverflowError
            objects = self.object_store.get(refstr).split()
            objects.reverse()
            stack.append(objects)

        while len(stack) > 0:
            top = stack[-1]
            if len(top) == 0:
                stack.pop()
                continue
            ref = top.pop()

            # An indirect reference which we must follow?
            if len(ref) > 0 and ref[0] == '@':
                follow_ref(ref[1:])
            else:
                yield ref

# Description of fields that might appear, and how they should be parsed.
MetadataItem.field_types = {
    'name': MetadataItem.decode_str,
    'type': MetadataItem.raw_str,
    'mode': MetadataItem.decode_int,
    'device': MetadataItem.decode_device,
    'user': MetadataItem.decode_user,
    'group': MetadataItem.decode_user,
    'ctime': MetadataItem.decode_int,
    'mtime': MetadataItem.decode_int,
    'links': MetadataItem.decode_int,
    'inode': MetadataItem.raw_str,
    'checksum': MetadataItem.decode_str,
    'size': MetadataItem.decode_int,
    'contents': MetadataItem.decode_str,
    'target': MetadataItem.decode_str,
}

def iterate_metadata(object_store, root):
    for d in parse(read_metadata(object_store, root), lambda l: len(l) == 0):
        yield MetadataItem(d, object_store)

class LocalDatabase:
    """Access to the local database of snapshot contents and object checksums.

    The local database is consulted when creating a snapshot to determine what
    data can be re-used from old snapshots.  Segment cleaning is performed by
    manipulating the data in the local database; the local database also
    includes enough data to guide the segment cleaning process.
    """

    def __init__(self, path, dbname="localdb.sqlite"):
        self.db_connection = sqlite3.connect(path + "/" + dbname)

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

    def list_schemes(self):
        """Return the list of snapshots found in the local database.

        The returned value is a list of tuples (id, scheme, name, time, intent).
        """

        cur = self.cursor()
        cur.execute("select distinct scheme from snapshots")
        schemes = [row[0] for row in cur.fetchall()]
        schemes.sort()
        return schemes

    def garbage_collect(self, scheme, intent=1.0):
        """Delete entries from old snapshots from the database.

        Only snapshots with the specified scheme name will be deleted.  If
        intent is given, it gives the intended next snapshot type, to determine
        how aggressively to clean (for example, intent=7 could be used if the
        next snapshot will be a weekly snapshot).
        """

        cur = self.cursor()

        # Get the list of old snapshots for this scheme.  Delete all the old
        # ones.  Rules for what to keep:
        #   - Always keep the most recent snapshot.
        #   - If snapshot X is younger than Y, and X has higher intent, then Y
        #     can be deleted.
        cur.execute("""select snapshotid, name, intent,
                              julianday('now') - timestamp as age
                       from snapshots where scheme = ?
                       order by age""", (scheme,))

        first = True
        max_intent = intent
        for (id, name, snap_intent, snap_age) in cur.fetchall():
            can_delete = False
            if snap_intent < max_intent:
                # Delete small-intent snapshots if there is a more recent
                # large-intent snapshot.
                can_delete = True
            elif snap_intent == intent:
                # Delete previous snapshots with the specified intent level.
                can_delete = True

            if can_delete and not first:
                print "Delete snapshot %d (%s)" % (id, name)
                cur.execute("delete from snapshots where snapshotid = ?",
                            (id,))
            first = False
            max_intent = max(max_intent, snap_intent)

        # Delete entries in the segments_used table which are for non-existent
        # snapshots.
        cur.execute("""delete from segments_used
                       where snapshotid not in
                           (select snapshotid from snapshots)""")

        # Find segments which contain no objects used by any current snapshots,
        # and delete them from the segment table.
        cur.execute("""delete from segments where segmentid not in
                           (select segmentid from segments_used)""")

        # Finally, delete objects contained in non-existent segments.  We can't
        # simply delete unused objects, since we use the set of unused objects
        # to determine the used/free ratio of segments.
        cur.execute("""delete from block_index
                       where segmentid not in
                           (select segmentid from segments)""")

    # Segment cleaning.
    class SegmentInfo(Struct): pass

    def get_segment_cleaning_list(self, age_boost=0.0):
        """Return a list of all current segments with information for cleaning.

        Return all segments which are currently known in the local database
        (there might be other, older segments in the archive itself), and
        return usage statistics for each to help decide which segments to
        clean.

        The returned list will be sorted by estimated cleaning benefit, with
        segments that are best to clean at the start of the list.

        If specified, the age_boost parameter (measured in days) will added to
        the age of each segment, as a way of adjusting the benefit computation
        before a long-lived snapshot is taken (for example, age_boost might be
        set to 7 when cleaning prior to taking a weekly snapshot).
        """

        cur = self.cursor()
        segments = []
        cur.execute("""select segmentid, used, size, mtime,
                       julianday('now') - mtime as age from segment_info""")
        for row in cur:
            info = self.SegmentInfo()
            info.id = row[0]
            info.used_bytes = row[1]
            info.size_bytes = row[2]
            info.mtime = row[3]
            info.age_days = row[4]

            # If age is not available for whatever reason, treat it as 0.0.
            if info.age_days is None:
                info.age_days = 0.0

            # Benefit calculation: u is the estimated fraction of each segment
            # which is utilized (bytes belonging to objects still in use
            # divided by total size; this doesn't take compression or storage
            # overhead into account, but should give a reasonable estimate).
            #
            # The total benefit is a heuristic that combines several factors:
            # the amount of space that can be reclaimed (1 - u), an ageing
            # factor (info.age_days) that favors cleaning old segments to young
            # ones and also is more likely to clean segments that will be
            # rewritten for long-lived snapshots (age_boost), and finally a
            # penalty factor for the cost of re-uploading data (u + 0.1).
            u = info.used_bytes / info.size_bytes
            info.cleaning_benefit \
                = (1 - u) * (info.age_days + age_boost) / (u + 0.1)

            segments.append(info)

        segments.sort(cmp, key=lambda s: s.cleaning_benefit, reverse=True)
        return segments

    def mark_segment_expired(self, segment):
        """Mark a segment for cleaning in the local database.

        The segment parameter should be either a SegmentInfo object or an
        integer segment id.  Objects in the given segment will be marked as
        expired, which means that any future snapshots that would re-use those
        objects will instead write out a new copy of the object, and thus no
        future snapshots will depend upon the given segment.
        """

        if isinstance(segment, int):
            id = segment
        elif isinstance(segment, self.SegmentInfo):
            id = segment.id
        else:
            raise TypeError("Invalid segment: %s, must be of type int or SegmentInfo, not %s" % (segment, type(segment)))

        cur = self.cursor()
        cur.execute("update block_index set expired = 1 where segmentid = ?",
                    (id,))

    def balance_expired_objects(self):
        """Analyze expired objects in segments to be cleaned and group by age.

        Update the block_index table of the local database to group expired
        objects by age.  The exact number of buckets and the cutoffs for each
        are dynamically determined.  Calling this function after marking
        segments expired will help in the segment cleaning process, by ensuring
        that when active objects from clean segments are rewritten, they will
        be placed into new segments roughly grouped by age.
        """

        # The expired column of the block_index table is used when generating a
        # new LBS snapshot.  A null value indicates that an object may be
        # re-used.  Otherwise, an object must be written into a new segment if
        # needed.  Objects with distinct expired values will be written into
        # distinct segments, to allow for some grouping by age.  The value 0 is
        # somewhat special in that it indicates any rewritten objects can be
        # placed in the same segment as completely new objects; this can be
        # used for very young objects which have been expired, or objects not
        # expected to be encountered.
        #
        # In the balancing process, all objects which are not used in any
        # current snapshots will have expired set to 0.  Objects which have
        # been seen will be sorted by age and will have expired values set to
        # 0, 1, 2, and so on based on age (with younger objects being assigned
        # lower values).  The number of buckets and the age cutoffs is
        # determined by looking at the distribution of block ages.

        cur = self.cursor()

        # Mark all expired objects with expired = 0; these objects will later
        # have values set to indicate groupings of objects when repacking.
        cur.execute("""update block_index set expired = 0
                       where expired is not null""")

        # We will want to aim for at least one full segment for each bucket
        # that we eventually create, but don't know how many bytes that should
        # be due to compression.  So compute the average number of bytes in
        # each expired segment as a rough estimate for the minimum size of each
        # bucket.  (This estimate could be thrown off by many not-fully-packed
        # segments, but for now don't worry too much about that.)  If we can't
        # compute an average, it's probably because there are no expired
        # segments, so we have no more work to do.
        cur.execute("""select avg(size) from segments
                       where segmentid in
                           (select distinct segmentid from block_index
                            where expired is not null)""")
        segment_size_estimate = cur.fetchone()[0]
        if not segment_size_estimate:
            return

        # Next, extract distribution of expired objects (number and size) by
        # age.  Save the timestamp for "now" so that the classification of
        # blocks into age buckets will not change later in the function, after
        # time has passed.  Set any timestamps in the future to now, so we are
        # guaranteed that for the rest of this function, age is always
        # non-negative.
        cur.execute("select julianday('now')")
        now = cur.fetchone()[0]

        cur.execute("""update block_index set timestamp = ?
                       where timestamp > ? and expired is not null""",
                    (now, now))

        cur.execute("""select round(? - timestamp) as age, count(*), sum(size)
                       from block_index where expired = 0
                       group by age order by age""", (now,))
        distribution = cur.fetchall()

        # Start to determine the buckets for expired objects.  Heuristics used:
        #   - An upper bound on the number of buckets is given by the number of
        #     segments we estimate it will take to store all data.  In fact,
        #     aim for a couple of segments per bucket.
        #   - Place very young objects in bucket 0 (place with new objects)
        #     unless there are enough of them to warrant a separate bucket.
        #   - Try not to create unnecessarily many buckets, since fewer buckets
        #     will allow repacked data to be grouped based on spatial locality
        #     (while more buckets will group by temporal locality).  We want a
        #     balance.
        MIN_AGE = 4
        total_bytes = sum([i[2] for i in distribution])
        target_buckets = 2 * (total_bytes / segment_size_estimate) ** 0.4
        min_size = 1.5 * segment_size_estimate
        target_size = max(2 * segment_size_estimate,
                          total_bytes / target_buckets)

        print "segment_size:", segment_size_estimate
        print "distribution:", distribution
        print "total_bytes:", total_bytes
        print "target_buckets:", target_buckets
        print "min, target size:", min_size, target_size

        # Chosen cutoffs.  Each bucket consists of objects with age greater
        # than one cutoff value, but not greater than the next largest cutoff.
        cutoffs = []

        # Starting with the oldest objects, begin grouping together into
        # buckets of size at least target_size bytes.
        distribution.reverse()
        bucket_size = 0
        min_age_bucket = False
        for (age, items, size) in distribution:
            if bucket_size >= target_size \
                or (age < MIN_AGE and not min_age_bucket):
                if bucket_size < target_size and len(cutoffs) > 0:
                    cutoffs.pop()
                cutoffs.append(age)
                bucket_size = 0

            bucket_size += size
            if age < MIN_AGE:
                min_age_bucket = True

        # The last (youngest) bucket will be group 0, unless it has enough data
        # to be of size min_size by itself, or there happen to be no objects
        # less than MIN_AGE at all.
        if bucket_size >= min_size or not min_age_bucket:
            cutoffs.append(-1)
        cutoffs.append(-1)

        print "cutoffs:", cutoffs

        # Update the database to assign each object to the appropriate bucket.
        cutoffs.reverse()
        for i in range(len(cutoffs)):
            cur.execute("""update block_index set expired = ?
                           where round(? - timestamp) > ?
                             and expired is not null""",
                        (i, now, cutoffs[i]))
