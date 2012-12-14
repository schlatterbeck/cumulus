-- We maintain a local index of data blocks that have been previously stored
-- for constructing incremental snapshots.
--
-- The index is stored in an SQLite3 database.  This is its schema.

-- Versioning information, describing the revision for which the table schema
-- was set up.
create table schema_version(
    version text,               -- Program version, dotted decimal string
    major integer,              -- Major version number
    minor integer               -- Minor version number
);
insert into schema_version values ('0.11', 0, 11);

-- List of snapshots which have been created and which we are still tracking.
-- There may be more snapshots than this actually stored at the remote server,
-- but the reverse should not ever be true: Cumulus may depend on data stored
-- in these snapshots when writing a new snapshot.
create table snapshots (
    snapshotid integer primary key,
    name text not null,
    scheme text not null,
    timestamp real,
    intent real                 -- TODO: deprecated, should be removed
);

-- List of segments which have been created.
create table segments (
    segmentid integer primary key,
    segment text unique not null,
    mtime real,                 -- timestamp when segment was created
    path text,
    checksum text,
    data_size integer,          -- sum of bytes in all objects in the segment
    disk_size integer,          -- size of segment on disk, after compression
    type text
);

-- Index of all data blocks in stored segments.  This is indexed by content
-- hash to allow for coarse block-level data deduplication.
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

-- Summary of segment utilization for each snapshot.
create table segment_utilization (
    snapshotid integer not null,
    segmentid integer not null,

    -- Estimate for the number of live bytes in data objects: this is capped at
    -- segments.data_size if all data in the segment is referenced.
    bytes_referenced integer
);
create unique index segment_utilization_index
    on segment_utilization(snapshotid, segmentid);
