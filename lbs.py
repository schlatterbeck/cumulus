"""High-level interface for working with LBS archives.

This module provides an easy interface for reading from and manipulating
various parts of an LBS archive:
  - listing the snapshots and segments present
  - reading segment contents
  - parsing snapshot descriptors and snapshot metadata logs
  - reading and maintaining the local object database
"""

from __future__ import division
from pysqlite2 import dbapi2 as sqlite3

class Struct:
    """A class which merely acts as a data container.

    Instances of this class (or its subclasses) are merely used to store data
    in various attributes.  No methods are provided.
    """

    def __repr__(self):
        return "<%s %s>" % (self.__class__, self.__dict__)

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

    def garbage_collect(self):
        """Delete entries from old snapshots from the database."""

        cur = self.cursor()

        # Delete old snapshots.
        cur.execute("""delete from snapshots
                       where snapshotid < (select max(snapshotid)
                                           from snapshots)""")

        # Delete entries in the snapshot_contents table which are for
        # non-existent snapshots.
        cur.execute("""delete from snapshot_contents
                       where snapshotid not in
                           (select snapshotid from snapshots)""")

        # Find segments which contain no objects used by any current snapshots,
        # and delete them from the segment table.
        cur.execute("""delete from segments where segmentid not in
                           (select distinct segmentid from snapshot_contents
                                natural join block_index)""")

        # Finally, delete objects contained in non-existent segments.  We can't
        # simply delete unused objects, since we use the set of unused objects
        # to determine the used/free ratio of segments.
        cur.execute("""delete from block_index
                       where segmentid not in
                           (select segmentid from segments)""")

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
                       julianday('now') - mtime as age from segment_info""")
        for row in cur:
            info = self.SegmentInfo()
            info.id = row[0]
            info.used_bytes = row[1]
            info.size_bytes = row[2]
            info.mtime = row[3]
            info.age_days = row[4]

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
        cur.execute("update block_index set expired = 1 where segmentid = ?",
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
        # new LBS snapshot.  A null value indicates that an object may be
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

        # First step: Mark all unused-and-expired objects with expired = -1,
        # which will cause us to mostly ignore these objects when rebalancing.
        # At the end, we will set these objects to be in group expired = 0.
        # Mark expired objects which still seem to be in use with expired = 0;
        # these objects will later have values set to indicate groupings of
        # objects when repacking.
        cur.execute("""update block_index set expired = -1
                       where expired is not null""")

        cur.execute("""update block_index set expired = 0
                       where expired is not null and blockid in
                           (select blockid from snapshot_contents)""")

        # We will want to aim for at least one full segment for each bucket
        # that we eventually create, but don't know how many bytes that should
        # be due to compression.  So compute the average number of bytes in
        # each expired segment as a rough estimate for the minimum size of each
        # bucket.  (This estimate could be thrown off by many not-fully-packed
        # segments, but for now don't worry too much about that.)  If we can't
        # compute an average, it's probably because there are no expired
        # segments, so we have no more work to do.
        cur.execute("""select avg(size) from segment_info
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

        print "segment_size:", segment_size_estimate
        print "distribution:", distribution
        print "total_bytes:", total_bytes
        print "target_buckets:", target_buckets
        print "min, target size:", min_size, target_size

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

        print "cutoffs:", cutoffs

        # Update the database to assign each object to the appropriate bucket.
        cutoffs.reverse()
        for i in range(len(cutoffs)):
            cur.execute("""update block_index set expired = ?
                           where round(? - timestamp) > ? and expired >= 0""",
                        (i, now, cutoffs[i]))
        cur.execute("update block_index set expired = 0 where expired = -1")
