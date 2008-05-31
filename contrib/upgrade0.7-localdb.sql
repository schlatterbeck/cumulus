-- SQL script for upgrading the local database to the format expected for LBS
-- version 0.7 (from a version 0.6 database).
--
-- This script should be loaded after connecting to the database to be
-- upgraded.

-- The subblock_signatures table was added to store a signature for old blocks
-- for performing subfile incremental backups.
create table subblock_signatures (
    blockid integer primary key,
    algorithm text not null,
    signatures blob not null
);
