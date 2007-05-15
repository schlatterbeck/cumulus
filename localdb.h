/* LBS: An LFS-inspired filesystem backup system
 * Copyright (C) 2007  Michael Vrable
 *
 * When creating backup snapshots, maintain a local database of data blocks and
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
    void Open(const char *path, const char *snapshot_name);
    void Close();
    void StoreObject(const ObjectReference& ref,
                     const std::string &checksum, int64_t size);
    ObjectReference FindObject(const std::string &checksum, int64_t size);
    void UseObject(const ObjectReference& ref);
private:
    std::string snapshot;
    sqlite3 *db;
};

#endif // _LBS_LOCALDB_H
