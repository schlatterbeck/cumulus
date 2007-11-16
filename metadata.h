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

#include <string>
#include <sstream>

#include "store.h"
#include "ref.h"
#include "util.h"

class MetadataWriter {
public:
    MetadataWriter(TarSegmentStore *store);
    void add(const std::string& path, dictionary info);
    ObjectReference close();

private:
    void metadata_flush();

    TarSegmentStore *store;
    std::ostringstream metadata, metadata_root;
};

#endif // _LBS_METADATA_H
