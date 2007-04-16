/* LBS: An LFS-inspired filesystem backup system
 * Copyright (C) 2006  Michael Vrable
 *
 * Backup data is stored in a collection of objects, which are grouped together
 * into segments for storage purposes.  This implementation of the object store
 * is built on top of libtar, and represents segments as TAR files and objects
 * as files within them. */

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <uuid/uuid.h>

#include <string>
#include <iostream>

#include "tarstore.h"

using std::string;

Tarfile::Tarfile(const string &path, const string &segment)
    : segment_name(segment)
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
}

string TarSegmentStore::write_object(const char *data, size_t len, const
                                     std::string &group)
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

    return segment->name + "/" + id_buf;
}

void TarSegmentStore::sync()
{
    while (!segments.empty()) {
        const string &name = segments.begin()->first;
        struct segment_info *segment = segments[name];

        fprintf(stderr, "Closing segment group %s (%s)\n",
                name.c_str(), segment->name.c_str());

        delete segment->file;
        segments.erase(segments.begin());
        delete segment;
    }
}
