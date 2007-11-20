/* LBS: An LFS-inspired filesystem backup system
 * Copyright (C) 2007  Michael Vrable
 *
 * Handling of metadata written to backup snapshots.  This manages the writing
 * of file metadata into new backup snapshots, including breaking the metadata
 * log apart across separate objects.  Eventually this should include unified
 * handling of the statcache, and re-use of metadata between snapshots.
 */

#include <string>
#include <iostream>

#include "metadata.h"
#include "store.h"
#include "statcache.h"
#include "util.h"

using std::list;
using std::string;
using std::ostream;
using std::ostringstream;

static const size_t LBS_METADATA_BLOCK_SIZE = 65536;

/* TODO: Move to header file */
void add_segment(const string& segment);

MetadataWriter::MetadataWriter(TarSegmentStore *store,
                               const char *path,
                               const char *snapshot_name,
                               const char *snapshot_scheme)
{
    statcache_path = path;
    statcache_path += "/statcache2";
    if (snapshot_scheme != NULL)
        statcache_path = statcache_path + "-" + snapshot_scheme;
    statcache_tmp_path = statcache_path + "." + snapshot_name;

    statcache_out = fopen(statcache_tmp_path.c_str(), "w");
    if (statcache_out == NULL) {
        fprintf(stderr, "Error opening statcache %s: %m\n",
                statcache_tmp_path.c_str());
        throw IOException("Error opening statcache");
    }

    this->store = store;
    chunk_size = 0;
}

/* Ensure contents of metadata are flushed to an object. */
void MetadataWriter::metadata_flush()
{
    int offset = 0;

    ostringstream metadata;
    for (list<MetadataItem>::iterator i = items.begin();
         i != items.end(); ++i) {
        metadata << i->text;
        i->offset = offset;
        offset += i->text.size();
    }
    string m = metadata.str();
    if (m.size() == 0)
        return;

    /* Write current metadata information to a new object. */
    LbsObject *meta = new LbsObject;
    meta->set_group("metadata");
    meta->set_data(m.data(), m.size());
    meta->write(store);
    meta->checksum();

    /* Write a reference to this block in the root. */
    ObjectReference ref = meta->get_ref();
    metadata_root << "@" << ref.to_string() << "\n";
    add_segment(ref.get_segment());

    delete meta;

    /* Write these files out to the statcache, and include a reference to where
     * the metadata lives (so we can re-use it if it has not changed). */
    for (list<MetadataItem>::const_iterator i = items.begin();
         i != items.end(); ++i) {
        ObjectReference r = ref;
        r.set_range(i->offset, i->text.size());

        string refstr = r.to_string();
        fprintf(statcache_out, "@@%s\n%s", refstr.c_str(), i->text.c_str());
    }

    items.clear();
}

void MetadataWriter::add(const string& path, dictionary info)
{
    MetadataItem item;
    item.offset = 0;
    item.text = "path: " + uri_encode(path) + "\n";
    item.text += encode_dict(info) + "\n";

    items.push_back(item);
    chunk_size += item.text.size();

    if (chunk_size > LBS_METADATA_BLOCK_SIZE)
        metadata_flush();
}

ObjectReference MetadataWriter::close()
{
    metadata_flush();
    const string root_data = metadata_root.str();

    LbsObject *root = new LbsObject;
    root->set_group("metadata");
    root->set_data(root_data.data(), root_data.size());
    root->write(store);
    root->checksum();
    add_segment(root->get_ref().get_segment());

    ObjectReference ref = root->get_ref();
    delete root;

    fclose(statcache_out);
    if (rename(statcache_tmp_path.c_str(), statcache_path.c_str()) < 0) {
        fprintf(stderr, "Error renaming statcache from %s to %s: %m\n",
                statcache_tmp_path.c_str(), statcache_path.c_str());
    }

    return ref;
}
