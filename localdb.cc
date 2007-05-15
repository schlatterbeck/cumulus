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

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <sqlite3.h>

#include <string>

#include "localdb.h"
#include "store.h"

using std::string;

void LocalDb::Open(const char *path, const char *snapshot_name)
{
    int rc;

    snapshot = snapshot_name;

    rc = sqlite3_open(path, &db);
    if (rc) {
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        throw IOException("Error opening local database");
    }

    rc = sqlite3_exec(db, "begin", NULL, NULL, NULL);
    if (rc) {
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        throw IOException("Error starting transaction");
    }
}

void LocalDb::Close()
{
    int rc;
    rc = sqlite3_exec(db, "commit", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Can't commit database!\n");
    }
    sqlite3_close(db);
}

void LocalDb::StoreObject(const ObjectReference& ref,
                          const string &checksum, int64_t size)
{
    int rc;
    sqlite3_stmt *stmt;
    static const char s[] =
        "insert into block_index(segment, object, checksum, size) "
        "values (?, ?, ?, ?)";
    const char *tail;

    rc = sqlite3_prepare_v2(db, s, strlen(s), &stmt, &tail);
    if (rc != SQLITE_OK) {
        return;
    }

    string seg = ref.get_segment();
    sqlite3_bind_text(stmt, 1, seg.c_str(), seg.size(), SQLITE_TRANSIENT);
    string obj = ref.get_sequence();
    sqlite3_bind_text(stmt, 2, obj.c_str(), obj.size(), SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, checksum.c_str(), checksum.size(),
                      SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 4, size);

    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        fprintf(stderr, "Could not execute INSERT statement!\n");
    }

    sqlite3_finalize(stmt);
}

ObjectReference LocalDb::FindObject(const string &checksum, int64_t size)
{
    int rc;
    sqlite3_stmt *stmt;
    static const char s[] =
        "select segment, object from block_index "
        "where checksum = ? and size = ?";
    const char *tail;

    ObjectReference ref;

    rc = sqlite3_prepare_v2(db, s, strlen(s), &stmt, &tail);
    if (rc != SQLITE_OK) {
        return ref;
    }

    sqlite3_bind_text(stmt, 1, checksum.c_str(), checksum.size(),
                      SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 2, size);

    rc = sqlite3_step(stmt);
    if (rc == SQLITE_DONE) {
    } else if (rc == SQLITE_ROW) {
        ref = ObjectReference((const char *)sqlite3_column_text(stmt, 0),
                              (const char *)sqlite3_column_text(stmt, 1));
    } else {
        fprintf(stderr, "Could not execute SELECT statement!\n");
    }

    sqlite3_finalize(stmt);

    return ref;
}

void LocalDb::UseObject(const ObjectReference& ref)
{
    int rc;
    sqlite3_stmt *stmt;
    static const char s[] =
        "insert into snapshot_contents "
        "select blockid, ? as snapshot from block_index "
        "where segment = ? and object = ?";
    const char *tail;

    rc = sqlite3_prepare_v2(db, s, strlen(s), &stmt, &tail);
    if (rc != SQLITE_OK) {
        return;
    }

    sqlite3_bind_text(stmt, 1, snapshot.c_str(), snapshot.size(),
                      SQLITE_TRANSIENT);
    string seg = ref.get_segment();
    sqlite3_bind_text(stmt, 2, seg.c_str(), seg.size(), SQLITE_TRANSIENT);
    string obj = ref.get_sequence();
    sqlite3_bind_text(stmt, 3, obj.c_str(), obj.size(), SQLITE_TRANSIENT);

    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        fprintf(stderr, "Could not execute INSERT statement!\n");
    }

    sqlite3_finalize(stmt);
}
