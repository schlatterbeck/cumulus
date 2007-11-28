/* LBS: An LFS-inspired filesystem backup system
 * Copyright (C) 2007  Michael Vrable
 *
 * Handling of metadata written to backup snapshots.  This manages the writing
 * of file metadata into new backup snapshots, including breaking the metadata
 * log apart across separate objects.  Eventually this should include unified
 * handling of the statcache, and re-use of metadata between snapshots.
 */

#ifndef _LBS_METADATA_H
#define _LBS_METADATA_H

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <list>
#include <string>
#include <sstream>

#include "store.h"
#include "ref.h"
#include "util.h"

/* Metadata for a single inode, ready to be written out. */
struct MetadataItem {
    int offset;
    std::string text;

    bool reused;
    ObjectReference ref;
};

class MetadataWriter {
public:
    MetadataWriter(TarSegmentStore *store, const char *path,
                   const char *snapshot_name, const char *snapshot_scheme);
    void add(dictionary info);
    ObjectReference close();

    bool find(const std::string& path);
    ObjectReference *old_ref() const {
        return ObjectReference::parse(old_metadata_loc);
    }

    bool matched() const { return found_match; }
    bool is_unchanged(const struct stat *stat_buf);

    dictionary get_old_metadata() const { return old_metadata; }
    std::list<ObjectReference> get_blocks();
    std::string get_checksum() { return old_metadata["checksum"]; }

private:
    void metadata_flush();
    void read_statcache();

    // Where are objects eventually written to?
    TarSegmentStore *store;

    // File descriptors for reading/writing local statcache data
    std::string statcache_path, statcache_tmp_path;
    FILE *statcache_in, *statcache_out;

    // Metadata not yet written out to the segment store
    size_t chunk_size;
    std::list<MetadataItem> items;
    std::ostringstream metadata_root;

    // Statcache information read back in from a previous run
    bool found_match;               // Result of last call to find
    bool old_metadata_eof;
    dictionary old_metadata;
    std::string old_metadata_loc;   // Reference to where the metadata is found
};

#endif // _LBS_METADATA_H
