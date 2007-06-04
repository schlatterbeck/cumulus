-- SQL commands for cleaning out the local database.  These commands should be
-- executed within a transaction, but this script does not do so
-- automatically--it is up to the caller to set up a transaction before
-- executing these commands.
--
-- Any snapshots which do not need to be tracked should be deleted from the
-- snapshots table prior to executing these commands.  This script will then
-- clean up all state with a snapshotid not found in the snapshots table.

-- Delete entries in the snapshot_contents table which are for non-existent
-- snapshots.
delete from snapshot_contents
    where snapshotid not in (select snapshotid from snapshots);

-- Find segments which contain no objects used by any current snapshots, and
-- delete them from the segment table.
delete from segments where segmentid not in
    (select distinct segmentid from snapshot_contents natural join block_index);

-- Finally, delete objects contained in non-existent segments.  We can't simply
-- delete unused objects, since we use the set of unused objects to determine
-- the used/free ratio of segments.
delete from block_index
    where segmentid not in (select segmentid from segments);
