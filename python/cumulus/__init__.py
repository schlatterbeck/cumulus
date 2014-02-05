# Cumulus: Efficient Filesystem Backup to the Cloud
# Copyright (C) 2008-2009, 2012 The Cumulus Developers
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

"""High-level interface for working with Cumulus archives.

This module provides an easy interface for reading from and manipulating
various parts of a Cumulus archive:
  - listing the snapshots and segments present
  - reading segment contents
  - parsing snapshot descriptors and snapshot metadata logs
  - reading and maintaining the local object database
"""

from __future__ import division, print_function, unicode_literals

import codecs
import hashlib
import itertools
import os
import re
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
try:
    import _thread
except ImportError:
    import thread as _thread

import cumulus.store
import cumulus.store.file

if sys.version < "3":
    StringTypes = (str, unicode)
else:
    StringTypes = (str,)

# The largest supported snapshot format that can be understood.
FORMAT_VERSION = (0, 11)        # Cumulus Snapshot v0.11

# Maximum number of nested indirect references allowed in a snapshot.
MAX_RECURSION_DEPTH = 3

# All segments which have been accessed this session.
accessed_segments = set()

# Table of methods used to filter segments before storage, and corresponding
# filename extensions.  These are listed in priority order (methods earlier in
# the list are tried first).
SEGMENT_FILTERS = [
    (".gpg", "cumulus-filter-gpg --decrypt"),
    (".gz", "gzip -dc"),
    (".bz2", "bzip2 -dc"),
    ("", None),
]

def to_lines(data):
    """Decode binary data from a file into a sequence of lines.

    Newline markers are retained."""
    return list(codecs.iterdecode(data.splitlines(True), "utf-8"))

def uri_decode(s):
    """Decode a URI-encoded (%xx escapes) string."""
    def hex_decode(m): return chr(int(m.group(1), 16))
    return re.sub(r"%([0-9a-f]{2})", hex_decode, s)
def uri_encode(s):
    """Encode a string to URI-encoded (%xx escapes) form."""
    def hex_encode(c):
        if c > '+' and c < '\x7f' and c != '@':
            return c
        else:
            return "%%%02x" % (ord(c),)
    return ''.join(hex_encode(c) for c in s)

class Struct:
    """A class which merely acts as a data container.

    Instances of this class (or its subclasses) are merely used to store data
    in various attributes.  No methods are provided.
    """

    def __repr__(self):
        return "<%s %s>" % (self.__class__, self.__dict__)

CHECKSUM_ALGORITHMS = {
    'sha1': hashlib.sha1,
    'sha224': hashlib.sha224,
    'sha256': hashlib.sha256,
}

class ChecksumCreator:
    """Compute a Cumulus checksum for provided data.

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

class SearchPathEntry(object):
    """Item representing a possible search location for Cumulus files.

    Some Cumulus files might be stored in multiple possible file locations: due
    to format (different compression mechanisms with different extensions),
    locality (different segments might be placed in different directories to
    control archiving policies), for backwards compatibility (default location
    changed over time).  A SearchPathEntry describes a possible location for a
    file.
    """
    def __init__(self, directory_prefix, suffix, context=None):
        self._directory_prefix = directory_prefix
        self._suffix = suffix
        self._context = context

    def __repr__(self):
        return "%s(%r, %r, %r)" % (self.__class__.__name__,
                                   self._directory_prefix, self._suffix,
                                   self._context)

    def build_path(self, basename):
        """Construct the search path to use for a file with name basename.

        Returns a tuple (pathname, context), where pathname is the path to try
        and context is any additional data associated with this search entry
        (if any).
        """
        return (os.path.join(self._directory_prefix, basename + self._suffix),
                self._context)

class SearchPath(object):
    """A collection of locations to search for files and lookup utilities.

    For looking for a file in a Cumulus storage backend, a SearchPath object
    contains a list of possible locations to try.  A SearchPath can be used to
    perform the search as well; when a file is found the search path ordering
    is updated (moving the successful SearchPathEntry to the front of the list
    for future searches).
    """
    def __init__(self, name_regex, searchpath):
        self._regex = re.compile(name_regex)
        self._path = list(searchpath)

    def add_search_entry(self, entry):
        self._path.append(entry)

    def directories(self):
        """Return the set of directories to search for a file type."""
        return set(entry._directory_prefix for entry in self._path)

    def get(self, backend, basename):
        for (i, entry) in enumerate(self._path):
            try:
                (pathname, context) = entry.build_path(basename)
                fp = backend.get(pathname)
                # On success, move this entry to the front of the search path
                # to speed future searches.
                if i > 0:
                    self._path.pop(i)
                    self._path.insert(0, entry)
                return (fp, pathname, context)
            except cumulus.store.NotFoundError:
                continue
        raise cumulus.store.NotFoundError(basename)

    def stat(self, backend, basename):
        for (i, entry) in enumerate(self._path):
            try:
                (pathname, context) = entry.build_path(basename)
                stat_data = backend.stat(pathname)
                # On success, move this entry to the front of the search path
                # to speed future searches.
                if i > 0:
                    self._path.pop(i)
                    self._path.insert(0, entry)
                result = {"path": pathname}
                result.update(stat_data)
                return result
            except cumulus.store.NotFoundError:
                continue
        raise cumulus.store.NotFoundError(basename)

    def match(self, filename):
        return self._regex.match(filename)

    def list(self, backend):
        success = False
        for d in self.directories():
            try:
                for f in backend.list(d):
                    success = True
                    m = self.match(f)
                    if m: yield (os.path.join(d, f), m)
            except cumulus.store.NotFoundError:
                pass
        if not success:
            raise cumulus.store.NotFoundError(backend)

def _build_segments_searchpath(prefix):
    for (extension, filter) in SEGMENT_FILTERS:
        yield SearchPathEntry(prefix, extension, filter)

SEARCH_PATHS = {
    "checksums": SearchPath(
        r"^snapshot-(.*)\.(\w+)sums$",
        [SearchPathEntry("meta", ".sha1sums"),
         SearchPathEntry("checksums", ".sha1sums"),
         SearchPathEntry("", ".sha1sums")]),
    "meta": SearchPath(
        r"^snapshot-(.*)\.meta(\.\S+)?$",
        _build_segments_searchpath("meta")),
    "segments": SearchPath(
        (r"^([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
         r"\.tar(\.\S+)?$"),
        itertools.chain(
            _build_segments_searchpath("segments0"),
            _build_segments_searchpath("segments1"),
            _build_segments_searchpath(""),
            _build_segments_searchpath("segments"))),
    "snapshots": SearchPath(
        r"^snapshot-(.*)\.(cumulus|lbs)$",
        [SearchPathEntry("snapshots", ".cumulus"),
         SearchPathEntry("snapshots", ".lbs"),
         SearchPathEntry("", ".cumulus"),
         SearchPathEntry("", ".lbs")]),
}

class BackendWrapper(object):
    """Wrapper around a Cumulus storage backend that understands file types.

    The BackendWrapper class understands different Cumulus file types, such as
    snapshots and segments, and implements higher-level operations such as
    "retrieve a snapshot with a specific name" (hiding operations such as
    searching for the correct file name).
    """

    def __init__(self, backend):
        """Initializes a wrapper around the specified storage backend.

        store may either be a Store object or URL.
        """
        if type(backend) in StringTypes:
            self._backend = cumulus.store.open(backend)
        else:
            self._backend = backend

    @property
    def raw_backend(self):
        return self._backend

    def stat_generic(self, basename, filetype):
        return SEARCH_PATHS[filetype].stat(self._backend, basename)

    def open_generic(self, basename, filetype):
        return SEARCH_PATHS[filetype].get(self._backend, basename)

    def open_snapshot(self, name):
        return self.open_generic("snapshot-" + name, "snapshots")

    def open_segment(self, name):
        return self.open_generic(name + ".tar", "segments")

    def list_generic(self, filetype):
        return ((x[1].group(1), x[0])
                for x in SEARCH_PATHS[filetype].list(self._backend))

    def prefetch_generic(self):
        """Calls scan on directories to prefetch file metadata."""
        directories = set()
        for typeinfo in SEARCH_PATHS.values():
            directories.update(typeinfo.directories())
        for d in directories:
            print("Prefetch", d)
            self._backend.scan(d)

class CumulusStore:
    def __init__(self, backend):
        if isinstance(backend, BackendWrapper):
            self.backend = backend
        else:
            self.backend = BackendWrapper(backend)
        self.cachedir = None
        self.CACHE_SIZE = 16
        self._lru_list = []

    def get_cachedir(self):
        if self.cachedir is None:
            self.cachedir = tempfile.mkdtemp("-cumulus")
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
            return ("zero", None, None, (0, int(m.group(1)), False))

        m = re.match(r"^([-0-9a-f]+)\/([0-9a-f]+)(\(\S+\))?(\[(=?(\d+)|(\d+)\+(\d+))\])?$", refstr)
        if not m: return

        segment = m.group(1)
        object = m.group(2)
        checksum = m.group(3)
        slice = m.group(4)

        if checksum is not None:
            checksum = checksum.lstrip("(").rstrip(")")

        if slice is not None:
            if m.group(6) is not None:
                # Size-assertion slice
                slice = (0, int(m.group(6)), True)
            else:
                slice = (int(m.group(7)), int(m.group(8)), False)

        return (segment, object, checksum, slice)

    def list_snapshots(self):
        return set(x[0] for x in self.backend.list_generic("snapshots"))

    def list_segments(self):
        return set(x[0] for x in self.backend.list_generic("segments"))

    def load_snapshot(self, snapshot):
        snapshot_file = self.backend.open_snapshot(snapshot)[0]
        return to_lines(snapshot_file.read())

    @staticmethod
    def filter_data(filehandle, filter_cmd):
        if filter_cmd is None:
            return filehandle
        p = subprocess.Popen(filter_cmd, shell=True, stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, close_fds=True)
        input, output = p.stdin, p.stdout
        def copy_thread(src, dst):
            BLOCK_SIZE = 4096
            while True:
                block = src.read(BLOCK_SIZE)
                if len(block) == 0: break
                dst.write(block)
            src.close()
            dst.close()
            p.wait()
        _thread.start_new_thread(copy_thread, (filehandle, input))
        return output

    def get_segment(self, segment):
        accessed_segments.add(segment)

        (segment_fp, path, filter_cmd) = self.backend.open_segment(segment)
        return self.filter_data(segment_fp, filter_cmd)

    def load_segment(self, segment):
        seg = tarfile.open(segment, 'r|', self.get_segment(segment))
        for item in seg:
            data_obj = seg.extractfile(item)
            path = item.name.split('/')
            if len(path) == 2 and path[0] == segment:
                yield (path[1], data_obj.read())

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
        if segment in self._lru_list: self._lru_list.remove(segment)
        self._lru_list.append(segment)
        while len(self._lru_list) > self.CACHE_SIZE:
            os.system("rm -rf " + os.path.join(self.cachedir,
                                               self._lru_list[0]))
            self._lru_list = self._lru_list[1:]
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
            (start, length, exact) = slice
            # Note: The following assertion check may need to be commented out
            # to restore from pre-v0.8 snapshots, as the syntax for
            # size-assertion slices has changed.
            if exact and len(data) != length: raise ValueError
            data = data[start:start+length]
            if len(data) != length: raise IndexError

        return data

    def prefetch(self):
        self.backend.prefetch_generic()

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

        m = re.match(r"^([-\w]+):\s*(.*)$", l)
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
        return next(parse(lines))
    except StopIteration:
        return {}

def parse_metadata_version(s):
    """Convert a string with the snapshot version format to a tuple."""

    m = re.match(r"^(?:Cumulus|LBS) Snapshot v(\d+(\.\d+)*)$", s)
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
        lines = to_lines(object_store.get(refstr))
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
        return uri_decode(s)

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

    def list_snapshots(self, scheme):
        """Return a list of snapshots for the given scheme."""
        cur = self.cursor()
        cur.execute("select name from snapshots")
        snapshots = [row[0] for row in cur.fetchall()]
        snapshots.sort()
        return snapshots

    def delete_snapshot(self, scheme, name):
        """Remove the specified snapshot from the database.

        Warning: This does not garbage collect all dependent data in the
        database, so it must be followed by a call to garbage_collect() to make
        the database consistent.
        """
        cur = self.cursor()
        cur.execute("delete from snapshots where scheme = ? and name = ?",
                    (scheme, name))

    def prune_old_snapshots(self, scheme, intent=1.0):
        """Delete entries from old snapshots from the database.

        Only snapshots with the specified scheme name will be deleted.  If
        intent is given, it gives the intended next snapshot type, to determine
        how aggressively to clean (for example, intent=7 could be used if the
        next snapshot will be a weekly snapshot).
        """

        cur = self.cursor()

        # Find the id of the last snapshot to be created.  This is used for
        # measuring time in a way: we record this value in each segment we
        # expire on this run, and then on a future run can tell if there have
        # been intervening backups made.
        cur.execute("select max(snapshotid) from snapshots")
        last_snapshotid = cur.fetchone()[0]

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
                print("Delete snapshot %d (%s)" % (id, name))
                cur.execute("delete from snapshots where snapshotid = ?",
                            (id,))
            first = False
            max_intent = max(max_intent, snap_intent)

        self.garbage_collect()

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
                       julianday('now') - mtime as age from segment_info
                       where expire_time is null""")
        for row in cur:
            info = self.SegmentInfo()
            info.id = row[0]
            info.used_bytes = row[1]
            info.size_bytes = row[2]
            info.mtime = row[3]
            info.age_days = row[4]

            # If data is not available for whatever reason, treat it as 0.0.
            if info.age_days is None:
                info.age_days = 0.0
            if info.used_bytes is None:
                info.used_bytes = 0.0

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
        cur.execute("select max(snapshotid) from snapshots")
        last_snapshotid = cur.fetchone()[0]
        cur.execute("update segments set expire_time = ? where segmentid = ?",
                    (last_snapshotid, id))
        cur.execute("update block_index set expired = 0 where segmentid = ?",
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
        # new Cumulus snapshot.  A null value indicates that an object may be
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

        print("segment_size:", segment_size_estimate)
        print("distribution:", distribution)
        print("total_bytes:", total_bytes)
        print("target_buckets:", target_buckets)
        print("min, target size:", min_size, target_size)

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

        print("cutoffs:", cutoffs)

        # Update the database to assign each object to the appropriate bucket.
        cutoffs.reverse()
        for i in range(len(cutoffs)):
            cur.execute("""update block_index set expired = ?
                           where round(? - timestamp) > ?
                             and expired is not null""",
                        (i, now, cutoffs[i]))
