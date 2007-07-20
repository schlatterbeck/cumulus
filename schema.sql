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
    segment text unique not null
);

-- Index of all blocks which have been stored in a snapshot, by checksum.
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

-- Index tracking which blocks are used by which snapshots.
create table snapshot_contents (
    blockid integer,
    snapshotid integer
);
create unique index snapshot_contents_unique
    on snapshot_contents(blockid, snapshotid);

-- Summary statistics for each segment.
create view segment_info as select * from
    (select segmentid, max(timestamp) as mtime,
            sum(size) as size, count(*) as objects
       from block_index natural join segments group by segmentid)
natural join
    (select segmentid, sum(size) as used, count(*) as objects_used
       from block_index where blockid in
            (select blockid from snapshot_contents) group by segmentid);

-- Ranking of segments to be cleaned, using a benefit function of
-- (fraction free space)*(age of youngest object).
create view cleaning_order as select *, (1-u)*age/(u+0.1) as benefit from
    (select segmentid,
            cast(used as real) / size as u, julianday('now') - mtime as age
        from segment_info)
where benefit > 0;
