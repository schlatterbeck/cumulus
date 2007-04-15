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
    tar_append_eof(t);

    if (tar_close(t) != 0)
        throw IOException("Error closing Tarfile");
}

void Tarfile::write_object(int id, const char *data, size_t len)
{
    memset(&t->th_buf, 0, sizeof(struct tar_header));

    char buf[64];
    sprintf(buf, "%08x", id);
    string path = segment_name + "/" + buf;
    printf("path: %s\n", path.c_str());

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

    th_print(t);

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
