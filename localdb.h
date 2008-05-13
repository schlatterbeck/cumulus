/* Cumulus: Smart Filesystem Backup to Dumb Servers
 *
 * Copyright (C) 2007-2008  The Regents of the University of California
 * Written by Michael Vrable <mvrable@cs.ucsd.edu>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

/* When creating backup snapshots, maintain a local database of data blocks and
 * checksums, in addition to the data contents (which may be stored remotely).
 * This database is consulted when attempting to build incremental snapshots,
 * as it says which objects can be reused.
 *
 * The database is implemented as an SQLite3 database, but this implementation
 * detail is kept internal to this file, so that the storage format may be
 * changed later. */

#ifndef _LBS_LOCALDB_H
#define _LBS_LOCALDB_H

#include <sqlite3.h>

#include <string>

#include "ref.h"

class LocalDb {
public:
    void Open(const char *path, const char *snapshot_name,
              const char *snapshot_scheme, double intent);
    void Close();
    void StoreObject(const ObjectReference& ref,
                     const std::string &checksum, int64_t size, double age);
    ObjectReference FindObject(const std::string &checksum, int64_t size);
    bool IsOldObject(const std::string &checksum, int64_t size, double *age,
                     int *group);
    bool IsAvailable(const ObjectReference &ref);
    void UseObject(const ObjectReference& ref);
    void UseSegment(const std::string &segment, double utilization);

    void SetSegmentChecksum(const std::string &segment, const std::string &path,
                            const std::string &checksum, int size);
    bool GetSegmentChecksum(const std::string &segment,
                            std::string *seg_path, std::string *seg_checksum);
private:
    sqlite3 *db;
    int64_t snapshotid;

    sqlite3_stmt *Prepare(const char *sql);
    void ReportError(int rc);
    int64_t SegmentToId(const std::string &segment);
    std::string IdToSegment(int64_t segmentid);
};

#endif // _LBS_LOCALDB_H
