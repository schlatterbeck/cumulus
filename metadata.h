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
};

class MetadataWriter {
public:
    MetadataWriter(TarSegmentStore *store, const char *path,
                   const char *snapshot_name, const char *snapshot_scheme);
    void add(const std::string& path, dictionary info);
    ObjectReference close();

private:
    void metadata_flush();

    // Where are objects eventually written to?
    TarSegmentStore *store;

    // File descriptors for reading/writing local statcache data
    std::string statcache_path, statcache_tmp_path;
    FILE *statcache_out;

    // Metadata not yet written out to the segment store
    size_t chunk_size;
    std::list<MetadataItem> items;
    std::ostringstream metadata_root;
};

#endif // _LBS_METADATA_H
