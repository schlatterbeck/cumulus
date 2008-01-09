-- SQL script for upgrading the local database to the format expected for LBS
-- version 0.6.
--
-- This script should be loaded after connecting to the database to be
-- upgraded.

-- Database schema changes: the size and mtime columns were added to the
-- segments table, and the segments_used table was added.  Rather than upgrade
-- the segments table in-place, we create a new table and then rename it over
-- the old segments table.  The intent column was also added to the snapshots
-- table.
create table segments_new (
    segmentid integer primary key,
    segment text unique not null,
    path text,
    checksum text,
    mtime real,
    size integer
);

create table segments_used (
    snapshotid integer not null,
    segmentid integer not null,
    utilization real
);
create unique index segments_used_index
    on segments_used(snapshotid, segmentid);

alter table snapshots add column intent real;

-- Initialize the intent column; set all old snapshots to have intent 1
-- (intended to be a daily snapshot).
update snapshots set intent = 1;

-- Compute the size of each of the segments, if possible, based on our
-- knowledge of the objects stored in them.
insert into segments_new
select segmentid, segment, path, checksum, mtime, size
from
    (select segmentid, segment, path, checksum from segments)
left join
    (select segmentid, sum(size) as size, max(timestamp) as mtime
     from block_index group by segmentid)
using (segmentid);

drop table segments;
alter table segments_new rename to segments;

-- Populate the segments_used table based upon data contained in
-- snapshot_contents--this is roughly the same calculation that is actually
-- done, only now this calculation is done when a snapshot is created.
insert into segments_used
select snapshotid, segmentid, cast(used as real) / size as utilization
from
    (select snapshotid, segmentid, sum(size) as used
     from snapshot_contents join block_index using (blockid)
     group by snapshotid, segmentid)
join
    (select segmentid, size from segments)
using (segmentid);

-- The snapshot_contents table is obsolete.
drop table snapshot_contents;

-- Upgrade database views.
drop view cleaning_order;
drop view segment_info;

create view segment_info as
select segmentid, mtime, size, cast(size * utilization as integer) as used,
       utilization
from segments join
     (select segmentid, max(utilization) as utilization
      from segments_used group by segmentid)
using (segmentid);
