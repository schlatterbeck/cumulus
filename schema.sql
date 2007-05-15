-- We maintain a local index of data blocks that have been previously stored
-- for constructing incremental snapshots.
--
-- The index is stored in an SQLite3 database.  This is its schema.

-- Index of all blocks which have been stored in one snapshot, by checksum.
create table block_index (
    blockid integer primary key,
    segment text,
    object text,
    checksum text,
    size integer
);
create index block_content_index on block_index(checksum);
