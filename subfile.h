/* Cumulus: Efficient Filesystem Backup to the Cloud
 * Copyright (C) 2008 The Cumulus Developers
 * See the AUTHORS file for a list of contributors.
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

/* Allow for sub-file incremental backups: if only a portion of a file changes,
 * allow the new data to be written out, and the old data to simply be
 * referenced from the new metadata log. */

#ifndef _LBS_SUBFILE_H
#define _LBS_SUBFILE_H

#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "localdb.h"
#include "ref.h"
#include "store.h"
#include "third_party/chunk.h"

class Subfile {
public:
    Subfile(LocalDb *localdb);
    ~Subfile();

    // Prepare to compute a subfile incremental by loading signatures for data
    // in the old file.
    void load_old_blocks(const std::list<ObjectReference> &blocks);

    // Break a new block of data into small chunks, and compute checksums of
    // the chunks.  After doing so, a delta can be computed, or the signatures
    // can be written out to the database.  The caller must not modify the
    // buffer until all operations referring to it are finished.
    void analyze_new_block(const char *buf, size_t len);

    // Store the signatures for the most recently-analyzed block in the local
    // database (linked to the specified object), if the block is sufficiently
    // large.  If signatures already exist, they will be overwritten.
    void store_analyzed_signatures(ObjectReference ref);

    std::list<ObjectReference> create_incremental(TarSegmentStore *tss,
                                                  LbsObject *o,
                                                  double block_age);

    static const int HASH_SIZE = 20;

private:
    struct chunk_info {
        char hash[HASH_SIZE];
        int offset, len;
    };

    struct block_summary {
        ObjectReference ref;
        int num_chunks;
        struct chunk_info *chunks;
    };

    LocalDb *db;
    bool checksums_loaded;
    std::set<ObjectReference> old_blocks;
    std::vector<block_summary> block_list;
    std::map<std::string, std::pair<int, int> > chunk_index;

    bool new_block_summary_valid;
    block_summary new_block_summary;

    const char *analyzed_buf;
    size_t analyzed_len;

    void ensure_signatures_loaded();
    void index_chunks(ObjectReference ref);
    void free_analysis();
    void store_block_signatures(ObjectReference ref, block_summary summary);

    std::string get_algorithm() {
        return chunk_algorithm_name() + "/sha1";
    }
};

#endif // _LBS_SUBFILE_H
