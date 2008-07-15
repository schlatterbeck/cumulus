-- We maintain a local index of data blocks that have been previously stored
-- for constructing incremental snapshots.
--
-- The index is stored in an SQLite3 database.  This is its schema.

-- List of snapshots which have been created.
create table snapshots (
    snapshotid integer primary key,
    name text not null,
    scheme text not null,
    timestamp real,
    intent real
);

-- List of segments which have been created.
create table segments (
    segmentid integer primary key,
    segment text unique not null,
    path text,
    checksum text,
    mtime real,
    size integer,
    expire_time integer         -- snapshotid of latest snapshot when expired
);

-- Index of all blocks which have been stored, by checksum.
create table block_index (
    blockid integer primary key,
    segmentid integer not null,
    object text not null,
    checksum text,
    size integer,
    timestamp real,
    expired integer
);
create index block_content_index on block_index(checksum);
create unique index block_name_index on block_index(segmentid, object);

-- Checksums for the decomposition of blocks into even smaller chunks
-- (variable-sized, but generally ~4 kB, and maximum 64 kB).  Chunk boundaries
-- are determined based on the contents using Rabin fingerprints.  These
-- checksums can be used for computing sub-file incrementals.
--
-- Each block stored in block_index may have an entry in the
-- subblock_signatures table.  The signatures field is a binary blob consisting
-- of a packed sequence of (chunk length [16-bit unsigned, big-endian],
-- checksum [20 bytes if SHA-1]) tuples that should cover the entire block.
--
-- algorithm specifies the method used for computing break points as well as
-- the hash function used, so that signatures can be discarded if the algorithm
-- changes.  The current algorithm used is 'lbfs-4096/sha1', which specifies a
-- target 4 kB block size with parameters set to match LBFS, and SHA-1 as the
-- hash algorithm.
create table subblock_signatures (
    blockid integer primary key,
    algorithm text not null,
    signatures blob not null
);

-- Summary of segment utilization for each snapshots.
create table segments_used (
    snapshotid integer not null,
    segmentid integer not null,
    utilization real
);
create unique index segments_used_index
    on segments_used(snapshotid, segmentid);

-- Overall estimate of segment utilization, for all snapshots combined.
create view segment_info as
select segmentid, mtime, size, expire_time,
       cast(size * utilization as integer) as used, utilization
from segments join
     (select segmentid, max(utilization) as utilization
      from segments_used group by segmentid)
using (segmentid);
