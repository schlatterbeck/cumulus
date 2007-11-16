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

using std::string;
using std::ostream;

static const size_t LBS_METADATA_BLOCK_SIZE = 65536;

/* TODO: Move to header file */
void add_segment(const string& segment);

MetadataWriter::MetadataWriter(TarSegmentStore *store)
{
    this->store = store;
}

/* Ensure contents of metadata are flushed to an object. */
void MetadataWriter::metadata_flush()
{
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

    metadata.str("");
}

void MetadataWriter::add(const string& path, dictionary info)
{
    metadata << "path: " << uri_encode(path) << "\n";
    metadata << encode_dict(info);
    metadata << "\n";

    if (metadata.str().size() > LBS_METADATA_BLOCK_SIZE)
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
    return ref;
}
