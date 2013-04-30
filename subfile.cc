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

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <arpa/inet.h>

#include "hash.h"
#include "subfile.h"
#include "third_party/chunk.h"

using std::list;
using std::map;
using std::set;
using std::string;
using std::vector;
using std::pair;
using std::make_pair;

Subfile::Subfile(LocalDb *localdb)
    : db(localdb), checksums_loaded(false), new_block_summary_valid(false)
{
    Hash *hasher = Hash::New();
    hasher->digest();
    algorithm_name = chunk_algorithm_name() + "/" + hasher->name();
    hash_size = hasher->digest_size();
    delete hasher;
}

Subfile::~Subfile()
{
    for (size_t i = 0; i < block_list.size(); i++) {
        delete[] block_list[i].chunks;
    }

    free_analysis();
}

void Subfile::free_analysis()
{
    if (new_block_summary_valid)
        delete[] new_block_summary.chunks;

    new_block_summary_valid = false;
}

void Subfile::load_old_blocks(const list<ObjectReference> &blocks)
{
    for (list<ObjectReference>::const_iterator i = blocks.begin();
         i != blocks.end(); ++i) {
        if (!i->is_normal())
            continue;

        ObjectReference base = i->base();
        if (old_blocks.find(base) == old_blocks.end()) {
            old_blocks.insert(base);
            if (checksums_loaded)
                index_chunks(base);
        }
    }
}

/* Actually load chunk signatures from the database, and index them in memory.
 * This should only be called once per segment. */
void Subfile::index_chunks(ObjectReference ref)
{
    string refstr = ref.to_string();

    if (!db->IsAvailable(ref))
        return;

    /* Look for checksums for this block in the database.  They may not exist,
     * in which case simply return without error. */
    char *packed_sigs;
    size_t len;
    string algorithm;
    if (!db->LoadChunkSignatures(ref, (void **)&packed_sigs, &len, &algorithm))
        return;
    if (algorithm != algorithm_name) {
        free(packed_sigs);
        return;
    }

    int block_id = block_list.size();

    block_summary summary;
    summary.ref = ref.base();
    summary.num_chunks = len / (2 + hash_size);
    summary.chunks = new chunk_info[summary.num_chunks];

    int block_start = 0;
    for (int i = 0; i < summary.num_chunks; i++) {
        char *packed_info = &packed_sigs[i * (2 + hash_size)];
        summary.chunks[i].hash = string(&packed_info[2], hash_size);

        uint16_t chunk_len;
        memcpy(&chunk_len, &packed_info[0], 2);
        summary.chunks[i].len = ntohs(chunk_len);
        summary.chunks[i].offset = block_start;
        block_start += summary.chunks[i].len;

        chunk_index[summary.chunks[i].hash] = make_pair(block_id, i);
    }

    block_list.push_back(summary);
    free(packed_sigs);
}

/* Signatures can be loaded lazily; this method should be called before any
 * actual access to the chunk signatures is required, to ensure the data has
 * actually been loaded. */
void Subfile::ensure_signatures_loaded()
{
    if (checksums_loaded)
        return;

    for (set<ObjectReference>::iterator i = old_blocks.begin();
         i != old_blocks.end(); ++i) {
        index_chunks(*i);
    }

    checksums_loaded = true;
}

void Subfile::analyze_new_block(const char *buf, size_t len)
{
    analyzed_buf = buf;
    analyzed_len = len;
    int max_chunks = chunk_compute_max_num_breaks(len);

    free_analysis();

    size_t *breakpoints = new size_t[max_chunks];
    int num_breakpoints = chunk_compute_breaks(buf, len, breakpoints);

    if (num_breakpoints == 0) {
        delete[] breakpoints;
        return;
    }

    new_block_summary.num_chunks = num_breakpoints;
    new_block_summary.chunks = new chunk_info[num_breakpoints];

    int block_start = 0;
    for (int i = 0; i < num_breakpoints; i++) {
        new_block_summary.chunks[i].offset = block_start;
        new_block_summary.chunks[i].len = breakpoints[i] - block_start + 1;
        block_start = breakpoints[i] + 1;

        Hash *hasher = Hash::New();
        hasher->update(&buf[new_block_summary.chunks[i].offset],
                       new_block_summary.chunks[i].len);
        new_block_summary.chunks[i].hash
            = string(reinterpret_cast<const char *>(hasher->digest()),
                     hasher->digest_size());
        delete hasher;
    }

    new_block_summary_valid = true;
    delete[] breakpoints;
}

void Subfile::store_block_signatures(ObjectReference ref, block_summary summary)
{
    int n = summary.num_chunks;
    char *packed = (char *)malloc(n * (2 + hash_size));

    for (int i = 0; i < n; i++) {
        assert(summary.chunks[i].len >= 0 && summary.chunks[i].len <= 0xffff);
        uint16_t len = htons(summary.chunks[i].len);
        char *packed_info = &packed[i * (2 + hash_size)];
        memcpy(&packed_info[0], &len, 2);
        memcpy(&packed_info[2], summary.chunks[i].hash.data(), hash_size);
    }

    db->StoreChunkSignatures(ref, packed, n * (2 + hash_size), algorithm_name);

    free(packed);
}

void Subfile::store_analyzed_signatures(ObjectReference ref)
{
    if (analyzed_len >= 16384)
        store_block_signatures(ref, new_block_summary);
}

/* Compute an incremental representation of the most recent block analyzed. */
enum subfile_item_type { SUBFILE_COPY, SUBFILE_NEW };

struct subfile_item {
    subfile_item_type type;

    // For type SUBFILE_COPY
    ObjectReference ref;

    // For type SUBFILE_NEW
    int src_offset, dst_offset;
    int len;
    string hash;
};

/* Compute an incremental representation of the data last analyzed.  A list of
 * references will be returned corresponding to the data.  If new data must be
 * written out to the backup, it will be written out via the LbsObject
 * provided, to the provided TarSegmentStore. */
list<ObjectReference> Subfile::create_incremental(TarSegmentStore *tss,
                                                  LbsObject *o,
                                                  double block_age)
{
    assert(new_block_summary_valid);
    bool matched_old = false;
    size_t new_data = 0;

    list<subfile_item> items;
    list<ObjectReference> refs;

    ensure_signatures_loaded();

    assert(new_block_summary.num_chunks > 0);

    for (int i = 0; i < new_block_summary.num_chunks; i++) {
        map<string, pair<int, int> >::iterator m
            = chunk_index.find(new_block_summary.chunks[i].hash);

        struct subfile_item item;
        if (m == chunk_index.end()) {
            item.type = SUBFILE_NEW;
            item.src_offset = new_block_summary.chunks[i].offset;
            item.dst_offset = new_data;
            item.len = new_block_summary.chunks[i].len;
            item.hash = new_block_summary.chunks[i].hash;
            new_data += item.len;
        } else {
            struct chunk_info &old_chunk
                = block_list[m->second.first].chunks[m->second.second];
            item.type = SUBFILE_COPY;
            item.ref = block_list[m->second.first].ref;
            item.ref.set_range(old_chunk.offset, old_chunk.len);
            matched_old = true;
        }

        items.push_back(item);
    }

    // No data was matched.  The entire block can be written out as is into a
    // new object, and the new_block_summary used to save chunk signatures.
    if (!matched_old) {
        o->set_age(block_age);
        o->set_data(analyzed_buf, analyzed_len, NULL);
        o->write(tss);
        ObjectReference ref = o->get_ref();
        store_analyzed_signatures(ref);
        refs.push_back(ref);
        delete o;
        return refs;
    }

    // Otherwise, construct a new block containing all literal data needed (if
    // any exists), write it out, and construct the appropriate list of
    // references.
    list<subfile_item>::iterator i;
    if (new_data > 0) {
        char *literal_buf = new char[new_data];
        for (i = items.begin(); i != items.end(); ++i) {
            if (i->type == SUBFILE_NEW) {
                memcpy(&literal_buf[i->dst_offset],
                       &analyzed_buf[i->src_offset], i->len);
            }
        }

        Hash *hasher = Hash::New();
        hasher->update(literal_buf, new_data);
        string block_csum = hasher->digest_str();
        delete hasher;

        o->set_group("data");
        o->set_data(literal_buf, new_data, NULL);
        o->write(tss);
        ObjectReference ref = o->get_ref();
        for (i = items.begin(); i != items.end(); ++i) {
            if (i->type == SUBFILE_NEW) {
                i->ref = ref;
                i->ref.set_range(i->dst_offset, i->len);
            }
        }

        //db->StoreObject(ref, 0.0);

        block_summary summary;
        summary.ref = ref;
        summary.num_chunks = 0;
        summary.chunks = new chunk_info[items.size()];
        for (i = items.begin(); i != items.end(); ++i) {
            if (i->type == SUBFILE_NEW) {
                chunk_info &info = summary.chunks[summary.num_chunks];
                info.hash = i->hash;
                info.offset = i->dst_offset;
                info.len = i->len;
                summary.num_chunks++;
            }
        }

        store_block_signatures(ref, summary);

        delete[] summary.chunks;
        delete[] literal_buf;
    }

    delete o;

    ObjectReference ref;
    for (i = items.begin(); i != items.end(); ++i) {
        string refstr = i->ref.to_string();
        if (!ref.merge(i->ref)) {
            refs.push_back(ref);
            ref = i->ref;
        }
    }
    assert(!ref.is_null());
    refs.push_back(ref);

    return refs;
}
