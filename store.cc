/* LBS: An LFS-inspired filesystem backup system
 * Copyright (C) 2007  Michael Vrable
 *
 * Backup data is stored in a collection of objects, which are grouped together
 * into segments for storage purposes.  This implementation of the object store
 * is built on top of libtar, and represents segments as TAR files and objects
 * as files within them. */

#include <assert.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <uuid/uuid.h>

#include <list>
#include <set>
#include <string>
#include <iostream>

#include "store.h"

using std::list;
using std::set;
using std::string;

list<string> TarSegmentStore::norefs;

Tarfile::Tarfile(const string &path, const string &segment)
    : size(0),
      segment_name(segment)
{
    if (tar_open(&t, (char *)path.c_str(), NULL, O_WRONLY | O_CREAT, 0600,
                 TAR_VERBOSE | TAR_GNU) == -1)
        throw IOException("Error opening Tarfile");
}

Tarfile::~Tarfile()
{
    string checksum_list = checksums.str();
    internal_write_object(segment_name + "/checksums",
                          checksum_list.data(), checksum_list.size());
    tar_append_eof(t);

    if (tar_close(t) != 0)
        throw IOException("Error closing Tarfile");
}

void Tarfile::write_object(int id, const char *data, size_t len)
{
    char buf[64];
    sprintf(buf, "%08x", id);
    string path = segment_name + "/" + buf;

    internal_write_object(path, data, len);

    // Compute a checksum for the data block, which will be stored at the end
    // of the TAR file.
    SHA1Checksum hash;
    hash.process(data, len);
    sprintf(buf, "%08x", id);
    checksums << buf << " " << hash.checksum_str() << "\n";
}

void Tarfile::internal_write_object(const string &path,
                                    const char *data, size_t len)
{
    memset(&t->th_buf, 0, sizeof(struct tar_header));

    th_set_type(t, S_IFREG | 0600);
    th_set_user(t, 0);
    th_set_group(t, 0);
    th_set_mode(t, 0600);
    th_set_size(t, len);
    th_set_mtime(t, time(NULL));
    th_set_path(t, const_cast<char *>(path.c_str()));
    th_finish(t);

    if (th_write(t) != 0)
        throw IOException("Error writing tar header");

    size += T_BLOCKSIZE;

    if (len == 0)
        return;

    size_t blocks = (len + T_BLOCKSIZE - 1) / T_BLOCKSIZE;
    size_t padding = blocks * T_BLOCKSIZE - len;

    for (size_t i = 0; i < blocks - 1; i++) {
        if (tar_block_write(t, &data[i * T_BLOCKSIZE]) == -1)
            throw IOException("Error writing tar block");
    }

    char block[T_BLOCKSIZE];
    memset(block, 0, sizeof(block));
    memcpy(block, &data[T_BLOCKSIZE * (blocks - 1)], T_BLOCKSIZE - padding);
    if (tar_block_write(t, block) == -1)
        throw IOException("Error writing final tar block");

    size += blocks * T_BLOCKSIZE;
}

static const size_t SEGMENT_SIZE = 4 * 1024 * 1024;

string TarSegmentStore::write_object(const char *data, size_t len, const
                                     std::string &group,
                                     const std::list<std::string> &refs)
{
    struct segment_info *segment;

    // Find the segment into which the object should be written, looking up by
    // group.  If no segment exists yet, create one.
    if (segments.find(group) == segments.end()) {
        segment = new segment_info;

        uint8_t uuid[16];
        char uuid_buf[40];
        uuid_generate(uuid);
        uuid_unparse_lower(uuid, uuid_buf);
        segment->name = uuid_buf;

        string filename = path + "/" + segment->name + ".tar";
        segment->file = new Tarfile(filename, segment->name);

        segment->count = 0;

        segments[group] = segment;
    } else {
        segment = segments[group];
    }

    int id = segment->count;
    char id_buf[64];
    sprintf(id_buf, "%08x", id);

    segment->file->write_object(id, data, len);
    segment->count++;

    string full_name = segment->name + "/" + id_buf;

    // Store any dependencies this object has on other segments, so they can be
    // written when the segment is closed.
    for (list<string>::const_iterator i = refs.begin(); i != refs.end(); ++i) {
        segment->refs.insert(*i);
    }

    // If this segment meets or exceeds the size target, close it so that
    // future objects will go into a new segment.
    if (segment->file->size_estimate() >= SEGMENT_SIZE)
        close_segment(group);

    return full_name;
}

void TarSegmentStore::sync()
{
    while (!segments.empty())
        close_segment(segments.begin()->first);
}

void TarSegmentStore::close_segment(const string &group)
{
    struct segment_info *segment = segments[group];
    fprintf(stderr, "Closing segment group %s (%s)\n",
            group.c_str(), segment->name.c_str());

    string reflist;
    for (set<string>::iterator i = segment->refs.begin();
         i != segment->refs.end(); ++i) {
        reflist += *i + "\n";
    }
    segment->file->internal_write_object(segment->name + "/references",
                                         reflist.data(), reflist.size());

    delete segment->file;
    segments.erase(segments.find(group));
    delete segment;
}

string TarSegmentStore::object_reference_to_segment(const string &object)
{
    return object;
}

LbsObject::LbsObject()
    : group(""), data(NULL), data_len(0), written(false)
{
}

LbsObject::~LbsObject()
{
}

void LbsObject::add_reference(const LbsObject *o)
{
    // TODO: Implement
}

void LbsObject::write(TarSegmentStore *store)
{
    assert(data != NULL);
    assert(!written);

    name = store->write_object(data, data_len, group);

    written = true;
    data = NULL;
}
