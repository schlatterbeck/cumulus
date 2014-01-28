#!/usr/bin/python
#
# Cumulus: Efficient Filesystem Backup to the Cloud
# Copyright (C) 2013 The Cumulus Developers
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

"""Code for rebuilding the local database.

Given a previous metadata dump and a filesystem tree (which may or may not
exactly match), recompute block signatures to the extent possible to rebuild
the local database.  This can be used to recover from a local database loss,
given data from a previous backup.
"""

import base64
import hashlib
import itertools
import os
import re
import struct
import subprocess
import sys
import tarfile
import time

import cumulus

CHECKSUM_ALGORITHM = "sha224"
CHUNKER_PROGRAM = "cumulus-chunker-standalone"

# TODO: Move to somewhere common
SQLITE_TIMESTAMP = "%Y-%m-%d %H:%M:%S"

class Chunker(object):
    """Compute sub-file chunk boundaries using a sliding Rabin fingerprint.

    This duplicates the chunking algorithm in third_party/chunk.cc.
    """
    # Chunking parameters.  These should match those in third_party/chunk.cc.
    MODULUS = 0xbfe6b8a5bf378d83
    WINDOW_SIZE = 48
    BREAKMARK_VALUE = 0x78
    MIN_CHUNK_SIZE = 2048
    MAX_CHUNK_SIZE = 65535
    TARGET_CHUNK_SIZE = 4096
    ALGORITHM_NAME = "lbfs-%d/%s" % (TARGET_CHUNK_SIZE, CHECKSUM_ALGORITHM)

    # Minimum size of a block before we should bother storing subfile
    # signatures (only applies when full blocks are used to store a file;
    # subfile signatures are always used when subfile incrementals are
    # present).
    MINIMUM_OBJECT_SIZE = 16384

    def __init__(self):
        degree = self.MODULUS.bit_length() - 1
        self.degree = degree

        # Lookup table for polynomial reduction when shifting a new byte in,
        # based on the high-order bits.
        self.T = [self.polymult(1, i << degree, self.MODULUS) ^ (i << degree)
                  for i in range(256)]

        # Values to remove a byte from the signature when it falls out of the
        # window.
        self.U = [self.polymult(i, 1 << 8*(self.WINDOW_SIZE - 1),
                                self.MODULUS) for i in range(256)]

        self.hash_algorithm = cumulus.CHECKSUM_ALGORITHMS[CHECKSUM_ALGORITHM]
        self.hash_size = self.hash_algorithm().digestsize

    def polymult(self, x, y, n):
        # Polynomial multiplication: result = x * y
        result = 0
        for i in range(x.bit_length()):
            if (x >> i) & 1:
                result ^= y << i
        # Reduction modulo n
        size = n.bit_length()
        while result.bit_length() >= size:
            result ^= n << (result.bit_length() - size)
        return result

    def window_init(self):
        # Sliding signature state is:
        #   [signature value, history buffer index, history buffer contents]
        return [0, 0, [0] * self.WINDOW_SIZE]

    def window_update(self, signature, byte):
        poly = signature[0]
        offset = signature[1]
        undo = self.U[signature[2][offset]]
        poly = ((poly ^ undo) << 8) + byte
        poly ^= self.T[poly >> self.degree]

        signature[0] = poly
        signature[1] = (offset + 1) % self.WINDOW_SIZE
        signature[2][offset] = byte

    def compute_breaks(self, buf):
        breaks = [0]
        signature = self.window_init()
        for i in range(len(buf)):
            self.window_update(signature, ord(buf[i]))
            block_len = i - breaks[-1] + 1
            if ((signature[0] % self.TARGET_CHUNK_SIZE == self.BREAKMARK_VALUE
                        and block_len >= self.MIN_CHUNK_SIZE)
                    or block_len >= self.MAX_CHUNK_SIZE):
                breaks.append(i + 1)
        if breaks[-1] < len(buf):
            breaks.append(len(buf))
        return breaks

    def compute_signatures(self, buf, buf_offset=0):
        """Break a buffer into chunks and compute chunk signatures.

        Args:
            buf: The data buffer.
            buf_offset: The offset of the data buffer within the original
                block, to handle cases where only a portion of the block is
                available.

        Returns:
            A dictionary containing signature data.  Keys are chunk offsets
            (from the beginning of the block), and values are tuples (size, raw
            hash value).
        """
        breaks = self.compute_breaks(buf)
        signatures = {}
        for i in range(1, len(breaks)):
            chunk = buf[breaks[i-1]:breaks[i]]
            hasher = self.hash_algorithm()
            hasher.update(chunk)
            signatures[breaks[i-1] + buf_offset] = (breaks[i] - breaks[i-1],
                                                    hasher.digest())
        return signatures

    def dump_signatures(self, signatures):
        """Convert signatures to the binary format stored in the database."""
        records = []

        # Emit records indicating that no signatures are available for the next
        # n bytes.  Since the size is a 16-bit value, to skip larger distances
        # multiple records must be emitted.  An all-zero signature indicates
        # the lack of data.
        def skip(n):
            while n > 0:
                i = min(n, self.MAX_CHUNK_SIZE)
                records.append(struct.pack(">H", i) + "\x00" * self.hash_size)
                n -= i

        position = 0
        for next_start, (size, digest) in sorted(signatures.items()):
            if next_start < position:
                print("Warning: overlapping signatures, ignoring")
                continue
            skip(next_start - position)
            records.append(struct.pack(">H", size) + digest)
            position = next_start + size

        return "".join(records)

    def load_signatures(self, signatures):
        """Loads signatures from the binary format stored in the database."""
        entry_size = 2 + self.hash_size
        if len(signatures) % entry_size != 0:
            print("Warning: Invalid signatures to load")
            return {}

        null_digest = "\x00" * self.hash_size
        position = 0
        result = {}
        for i in range(len(signatures) // entry_size):
            sig = signatures[i*entry_size:(i+1)*entry_size]
            size, digest = struct.unpack(">H", sig[:2])[0], sig[2:]
            if digest != null_digest:
                result[position] = (size, digest)
            position += size
        return result


class ChunkerExternal(Chunker):
    """A Chunker which uses an external program to compute Rabin fingerprints.

    This can run much more quickly than the Python code, but should otherwise
    give identical results.
    """

    def __init__(self):
        super(ChunkerExternal, self).__init__()
        self.subproc = subprocess.Popen([CHUNKER_PROGRAM],
                                        stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE)

    def compute_breaks(self, buf):
        if len(buf) == 0:
            return [0]
        self.subproc.stdin.write(struct.pack(">i", len(buf)))
        self.subproc.stdin.write(buf)
        self.subproc.stdin.flush()
        breaks = self.subproc.stdout.readline()
        return [0] + [int(x) + 1 for x in breaks.split()]


class DatabaseRebuilder(object):
    def __init__(self, database):
        self.database = database
        self.cursor = database.cursor()
        self.segment_ids = {}
        self.chunker = ChunkerExternal()
        #self.chunker = Chunker()

    def segment_to_id(self, segment):
        if segment in self.segment_ids: return self.segment_ids[segment]

        self.cursor.execute("""insert or ignore into segments(segment)
                               values (?)""", (segment,))
        self.cursor.execute("""select segmentid from segments
                               where segment = ?""", (segment,))
        id = self.cursor.fetchone()[0]
        self.segment_ids[segment] = id
        return id

    def rebuild(self, metadata, reference_path):
        """Iterate through old metadata and use it to rebuild the database.

        Args:
            metadata: An iterable containing lines of the metadata log.
            reference_path: Path to the root of a file system tree which may be
                similar to data in the metadata log (used to recompute block
                signatures).
        """
        for fields in cumulus.parse(metadata, lambda l: len(l) == 0):
            metadata = cumulus.MetadataItem(fields, None)
            # Only process regular files; skip over other types (directories,
            # symlinks, etc.)
            if metadata.items.type not in ("-", "f"): continue
            try:
                path = os.path.join(reference_path, metadata.items.name)
                print("Path:", path)
                # TODO: Check file size for early abort if different
                self.rebuild_file(open(path), metadata)
            except IOError as e:
                print(e)
                pass  # Ignore the file

        self.database.commit()

    def reload_segment_metadata(self, segment_metadata):
        """Read a segment metadata (.meta) file into the local database.

        Updates the segments table in the local database with information from
        a a segment metadata backup file.  Old data is not overwritten, so
        loading a .meta file with partial information is fine.
        """
        for info in cumulus.parse(segment_metadata,
                                     terminate=lambda l: len(l) == 0):
            segment = info.pop("segment")
            self.insert_segment_info(segment, info)

        self.database.commit()

    def insert_segment_info(self, segment, info):
        id = self.segment_to_id(segment)
        for k, v in list(info.items()):
            self.cursor.execute("update segments set " + k + " = ? "
                                "where segmentid = ?",
                                (v, id))

    def rebuild_file(self, fp, metadata):
        """Recompute database signatures if a file is unchanged.

        If the current file contents match that from the old metadata (the
        full-file hash matches), then recompute block- and chunk-level
        signatures for the objects referenced by the file.
        """
        blocks = [cumulus.CumulusStore.parse_ref(b) for b in metadata.data()]
        verifier = cumulus.ChecksumVerifier(metadata.items.checksum)
        checksums = {}
        subblock = {}
        for segment, object, checksum, slice in blocks:
            # Given a reference to a block of unknown size we don't know how to
            # match up the data, so we have to give up on rebuilding for this
            # file.
            if slice is None: return

            start, length, exact = slice
            buf = fp.read(length)
            verifier.update(buf)

            # Zero blocks get no checksums, so skip further processing on them.
            if object is None: continue

            if exact:
                csum = cumulus.ChecksumCreator(CHECKSUM_ALGORITHM)
                csum.update(buf)
                checksums[(segment, object)] = (length, csum.compute())
            else:
                # Compute a lower bound on the object size.
                oldlength, csum = checksums.get((segment, object), (0, None))
                checksums[(segment, object)] = (max(oldlength, start + length),
                                                csum)

            if length >= self.chunker.MINIMUM_OBJECT_SIZE or not exact:
                signatures = self.chunker.compute_signatures(buf, start)
                subblock.setdefault((segment, object), {}).update(signatures)

        if verifier.valid():
            for k in subblock:
                subblock[k] = self.chunker.dump_signatures(subblock[k])
            self.store_checksums(checksums, subblock)
        else:
            print("Checksum mismatch")

    def store_checksums(self, block_checksums, subblock_signatures):
        for (segment, object), (size, checksum) in block_checksums.items():
            segmentid = self.segment_to_id(segment)
            self.cursor.execute(
                """insert or ignore into block_index(segmentid, object)
                   values (?, ?)""",
                (segmentid, object))
            self.cursor.execute("""select blockid from block_index
                                   where segmentid = ? and object = ?""",
                                (segmentid, object))
            blockid = self.cursor.fetchall()[0][0]

            # Store checksum only if it is available; we don't want to
            # overwrite an existing checksum in the database with NULL.
            if checksum is not None:
                self.cursor.execute("""update block_index
                                       set checksum = ?
                                       where blockid = ?""",
                                    (checksum, blockid))

            # Update the object size.  Our size may be an estimate, based on
            # slices that we have seen.  The size in the database must not be
            # larger than the true size, but update it to the largest value
            # possible.
            self.cursor.execute("""update block_index
                                   set size = max(?, coalesce(size, 0))
                                   where blockid = ?""",
                                (size, blockid))

            # Store subblock signatures, if available.
            # TODO: Even better would be to merge signature data, to handle the
            # case where different files see different chunks of a block.
            sigs = subblock_signatures.get((segment, object))
            if sigs:
                self.cursor.execute(
                    """insert or replace into subblock_signatures(
                           blockid, algorithm, signatures)
                       values (?, ?, ?)""",
                    (blockid, self.chunker.ALGORITHM_NAME, buffer(sigs)))


class SegmentStateRebuilder(object):
    """Reconstructs segment metadata files from raw segment data."""

    def __init__(self):
        self.filters = dict(cumulus.SEGMENT_FILTERS)
        self.segment_pattern = cumulus.SEARCH_PATHS["segments"]

    def compute_metadata(self, path, relative_path):
        """Recompute metadata of a single segment.

        Args:
            path: Path to the segment file in the file system.
            relative_path: Path relative to the root for the storage backend.
        """
        # Does the filename match that of a segment?  If so, extract the
        # extension to determine the filter to apply to decompress.
        filename = os.path.basename(relative_path)
        m = self.segment_pattern.match(filename)
        if not m: return
        segment_name = m.group(1)
        extension = m.group(2)
        if extension not in self.filters: return
        filter_cmd = self.filters[extension]

        # File attributes.
        st_buf = os.stat(path)
        timestamp = time.strftime(SQLITE_TIMESTAMP,
                                  time.gmtime(st_buf.st_mtime))

        # Compute attributes of the compressed segment data.
        BLOCK_SIZE = 4096
        with open(path) as segment:
            disk_size = 0
            checksummer = cumulus.ChecksumCreator(CHECKSUM_ALGORITHM)
            while True:
                buf = segment.read(BLOCK_SIZE)
                if len(buf) == 0: break
                disk_size += len(buf)
                checksummer.update(buf)
        checksum = checksummer.compute()

        # Compute attributes of the objects within the segment.
        data_size = 0
        object_count = 0
        with open(path) as segment:
            decompressed = cumulus.CumulusStore.filter_data(segment, filter_cmd)
            objects = tarfile.open(mode='r|', fileobj=decompressed)
            for tarinfo in objects:
                data_size += tarinfo.size
                object_count += 1

        return {"segment": cumulus.uri_encode(segment_name),
                "path": cumulus.uri_encode(relative_path),
                "checksum": checksum,
                "data_size": data_size,
                "disk_size": disk_size,
                "timestamp": timestamp}

if __name__ == "__main__":
    # Sample code to reconstruct segment metadata--ought to be relocated.
    if False:
        segment_rebuilder = SegmentStateRebuilder()
        topdir = sys.argv[1]
        files = []
        for dirpath, dirnames, filenames in os.walk(topdir):
            for f in filenames:
                files.append(os.path.join(dirpath, f))
        files.sort()
        for f in files:
            metadata = segment_rebuilder.compute_metadata(
                f,
                os.path.relpath(f, topdir))
            if metadata:
                for (k, v) in sorted(metadata.items()):
                    print("%s: %s" % (k, v))
                print()
        sys.exit(0)

    # Sample code to rebuild the segments table from metadata--needs to be
    # merged with the code below.
    if False:
        rebuilder = DatabaseRebuilder(cumulus.LocalDatabase(sys.argv[1]))
        rebuilder.reload_segment_metadata(open(sys.argv[2]))
        sys.exit(0)

    # Read metadata from stdin; filter out lines starting with "@@" so the
    # statcache file can be parsed as well.
    metadata = (x for x in sys.stdin if not x.startswith("@@"))

    rebuilder = DatabaseRebuilder(cumulus.LocalDatabase(sys.argv[1]))
    rebuilder.rebuild(metadata, "/")
