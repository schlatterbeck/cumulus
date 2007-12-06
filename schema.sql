-- We maintain a local index of data blocks that have been previously stored
-- for constructing incremental snapshots.
--
-- The index is stored in an SQLite3 database.  This is its schema.

-- List of snapshots which have been created.
create table snapshots (
    snapshotid integer primary key,
    name text not null,
    scheme text,
    timestamp real
);

-- List of segments which have been created.
create table segments (
    segmentid integer primary key,
    segment text unique not null,
    path text,
    checksum text,
    size integer
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

-- Summary of segment utilization for each snapshots.
create table segments_used (
    snapshotid integer not null,
    segmentid integer not null,
    utilization real
);

-- Index tracking which blocks are used by which snapshots.
create table snapshot_contents (
    blockid integer,
    snapshotid integer
);
create unique index snapshot_contents_unique
    on snapshot_contents(blockid, snapshotid);
