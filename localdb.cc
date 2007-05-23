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

    /* Insert this snapshot into the database, and determine the integer key
     * which will be used to identify it. */
    sqlite3_stmt *stmt;
    static const char s[] =
        "insert into snapshots(name, timestamp) "
        "values (?, julianday('now'))";
    const char *tail;

    rc = sqlite3_prepare_v2(db, s, strlen(s), &stmt, &tail);
    if (rc != SQLITE_OK) {
        sqlite3_close(db);
        throw IOException("Error adding snapshot");
    }

    sqlite3_bind_text(stmt, 1, snapshot_name, strlen(snapshot_name),
                      SQLITE_TRANSIENT);

    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        sqlite3_close(db);
        throw IOException("Database execution error!");
    }

    snapshotid = sqlite3_last_insert_rowid(db);
    sqlite3_finalize(stmt);
    if (snapshotid == 0) {
        sqlite3_close(db);
        throw IOException("Find snapshot id");
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

int64_t LocalDb::SegmentToId(const string &segment)
{
    int rc;
    sqlite3_stmt *stmt;
    static const char s1[] =
        "insert or ignore into segments(segment) values (?);";
    static const char s2[] =
        "select segmentid from segments where segment = ?";
    const char *tail;
    int64_t result;

    rc = sqlite3_prepare_v2(db, s1, strlen(s1), &stmt, &tail);
    if (rc != SQLITE_OK) {
        throw IOException("Find id by segment name");
    }
    sqlite3_bind_text(stmt, 1, segment.c_str(), segment.size(),
                      SQLITE_TRANSIENT);
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        throw IOException("Could not execute INSERT statement!");
    }
    sqlite3_finalize(stmt);

    rc = sqlite3_prepare_v2(db, s2, strlen(s2), &stmt, &tail);
    if (rc != SQLITE_OK) {
        throw IOException("Find id by segment name");
    }

    sqlite3_bind_text(stmt, 1, segment.c_str(), segment.size(),
                      SQLITE_TRANSIENT);

    rc = sqlite3_step(stmt);
    if (rc == SQLITE_DONE) {
        throw IOException("No segment found by id");
    } else if (rc == SQLITE_ROW) {
        result = sqlite3_column_int64(stmt, 0);
    } else {
        throw IOException("Error executing find segment by id query");
    }

    sqlite3_finalize(stmt);

    return result;
}

string LocalDb::IdToSegment(int64_t segmentid)
{
    int rc;
    sqlite3_stmt *stmt;
    static const char s[] =
        "select segment from segments where segmentid = ?";
    const char *tail;
    string result;

    rc = sqlite3_prepare_v2(db, s, strlen(s), &stmt, &tail);
    if (rc != SQLITE_OK) {
        throw IOException("Find segment by id");
    }

    sqlite3_bind_int64(stmt, 1, segmentid);

    rc = sqlite3_step(stmt);
    if (rc == SQLITE_DONE) {
        throw IOException("No segment found by id");
    } else if (rc == SQLITE_ROW) {
        result = (const char *)sqlite3_column_text(stmt, 0);
    } else {
        throw IOException("Error executing find segment by id query");
    }

    sqlite3_finalize(stmt);

    return result;
}

void LocalDb::StoreObject(const ObjectReference& ref,
                          const string &checksum, int64_t size)
{
    int rc;
    sqlite3_stmt *stmt;
    static const char s[] =
        "insert into "
        "block_index(segmentid, object, checksum, size, timestamp) "
        "values (?, ?, ?, ?, julianday('now'))";
    const char *tail;

    rc = sqlite3_prepare_v2(db, s, strlen(s), &stmt, &tail);
    if (rc != SQLITE_OK) {
        return;
    }

    sqlite3_bind_int64(stmt, 1, SegmentToId(ref.get_segment()));
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
        "select segmentid, object from block_index "
        "where checksum = ? and size = ? and expired is null";
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
        ref = ObjectReference(IdToSegment(sqlite3_column_int64(stmt, 0)),
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
        "insert or ignore into snapshot_contents "
        "select blockid, ? as snapshotid from block_index "
        "where segmentid = ? and object = ?";
    const char *tail;

    rc = sqlite3_prepare_v2(db, s, strlen(s), &stmt, &tail);
    if (rc != SQLITE_OK) {
        return;
    }

    sqlite3_bind_int64(stmt, 1, snapshotid);
    sqlite3_bind_int64(stmt, 2, SegmentToId(ref.get_segment()));
    string obj = ref.get_sequence();
    sqlite3_bind_text(stmt, 3, obj.c_str(), obj.size(), SQLITE_TRANSIENT);

    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        fprintf(stderr, "Could not execute INSERT statement!\n");
    }

    sqlite3_finalize(stmt);
}
