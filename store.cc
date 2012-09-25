/* Cumulus: Efficient Filesystem Backup to the Cloud
 * Copyright (C) 2008-2009 The Cumulus Developers
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

/* Backup data is stored in a collection of objects, which are grouped together
 * into segments for storage purposes.  This implementation of the object store
 * represents segments as TAR files and objects as files within them. */

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>

#include <algorithm>
#include <list>
#include <map>
#include <set>
#include <string>
#include <iostream>

#include "hash.h"
#include "store.h"
#include "ref.h"
#include "util.h"

using std::max;
using std::list;
using std::map;
using std::pair;
using std::set;
using std::string;

/* Default filter program is bzip2 */
const char *filter_program = "bzip2 -c";
const char *filter_extension = ".bz2";

Tarfile::Tarfile(RemoteFile *file, const string &segment)
    : size(0),
      segment_name(segment)
{
    assert(sizeof(struct tar_header) == TAR_BLOCK_SIZE);

    this->file = file;
    real_fd = file->get_fd();
    filter_fd = spawn_filter(real_fd, filter_program, &filter_pid);
}

Tarfile::~Tarfile()
{
    char buf[TAR_BLOCK_SIZE];

    /* Append the EOF marker: two blocks filled with nulls. */
    memset(buf, 0, sizeof(buf));
    tar_write(buf, TAR_BLOCK_SIZE);
    tar_write(buf, TAR_BLOCK_SIZE);

    if (close(filter_fd) != 0)
        fatal("Error closing Tarfile");

    /* ...and wait for filter process to finish. */
    int status;
    waitpid(filter_pid, &status, 0);

    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fatal("Filter process error");
    }

    close(real_fd);
}

/* Launch a child process which can act as a filter (compress, encrypt, etc.)
 * on the TAR output.  The file descriptor to which output should be written
 * must be specified; the return value is the file descriptor which will be
 * attached to the standard input of the filter program. */
int spawn_filter(int fd_out, const char *program, pid_t *filter_pid)
{
    int fds[2];
    pid_t pid;

    /* Create a pipe for communicating with the filter process. */
    if (pipe(fds) < 0) {
        fatal("Unable to create pipe for filter");
    }

    /* Create a child process which can exec() the filter program. */
    pid = fork();
    if (pid < 0)
        fatal("Unable to fork filter process");

    if (pid > 0) {
        /* Parent process */
        close(fds[0]);
        cloexec(fds[1]);
        if (filter_pid != NULL)
            *filter_pid = pid;
    } else {
        /* Child process.  Rearrange file descriptors.  stdin is fds[0], stdout
         * is fd_out, stderr is unchanged. */
        close(fds[1]);

        if (dup2(fds[0], 0) < 0)
            exit(1);
        close(fds[0]);

        if (dup2(fd_out, 1) < 0)
            exit(1);
        close(fd_out);

        /* Exec the filter program. */
        execlp("/bin/sh", "/bin/sh", "-c", program, NULL);

        /* Should not reach here except for error cases. */
        fprintf(stderr, "Could not exec filter: %m\n");
        exit(1);
    }

    return fds[1];
}

void Tarfile::tar_write(const char *data, size_t len)
{
    size += len;

    while (len > 0) {
        int res = write(filter_fd, data, len);

        if (res < 0) {
            if (errno == EINTR)
                continue;
            fprintf(stderr, "Write error: %m\n");
            fatal("Write error");
        }

        len -= res;
        data += res;
    }
}

void Tarfile::write_object(int id, const char *data, size_t len)
{
    struct tar_header header;
    memset(&header, 0, sizeof(header));

    char buf[64];
    sprintf(buf, "%08x", id);
    string path = segment_name + "/" + buf;

    assert(path.size() < 100);
    memcpy(header.name, path.data(), path.size());
    sprintf(header.mode, "%07o", 0600);
    sprintf(header.uid, "%07o", 0);
    sprintf(header.gid, "%07o", 0);
    sprintf(header.size, "%011o", (int)len);
    sprintf(header.mtime, "%011o", (int)time(NULL));
    header.typeflag = '0';
    strcpy(header.magic, "ustar  ");
    strcpy(header.uname, "root");
    strcpy(header.gname, "root");

    memset(header.chksum, ' ', sizeof(header.chksum));
    int checksum = 0;
    for (int i = 0; i < TAR_BLOCK_SIZE; i++) {
        checksum += ((uint8_t *)&header)[i];
    }
    sprintf(header.chksum, "%06o", checksum);

    tar_write((const char *)&header, TAR_BLOCK_SIZE);

    if (len == 0)
        return;

    tar_write(data, len);

    char padbuf[TAR_BLOCK_SIZE];
    size_t blocks = (len + TAR_BLOCK_SIZE - 1) / TAR_BLOCK_SIZE;
    size_t padding = blocks * TAR_BLOCK_SIZE - len;
    memset(padbuf, 0, padding);
    tar_write(padbuf, padding);
}

/* Estimate the size based on the size of the actual output file on disk.
 * However, it might be the case that the filter program is buffering all its
 * data, and might potentially not write a single byte until we have closed
 * our end of the pipe.  If we don't do so until we see data written, we have
 * a problem.  So, arbitrarily pick an upper bound on the compression ratio
 * that the filter will achieve (128:1), and return a size estimate which is
 * the larger of a) bytes actually seen written to disk, and b) input
 * bytes/128. */
size_t Tarfile::size_estimate()
{
    struct stat statbuf;

    if (fstat(real_fd, &statbuf) == 0)
        return max((int64_t)statbuf.st_size, (int64_t)(size / 128));

    /* Couldn't stat the file on disk, so just return the actual number of
     * bytes, before compression. */
    return size;
}

static const size_t SEGMENT_SIZE = 4 * 1024 * 1024;

/* Backup size summary: segment type -> (uncompressed size, compressed size) */
static map<string, pair<int64_t, int64_t> > group_sizes;

ObjectReference TarSegmentStore::write_object(const char *data, size_t len,
                                              const std::string &group)
{
    struct segment_info *segment;

    // Find the segment into which the object should be written, looking up by
    // group.  If no segment exists yet, create one.
    if (segments.find(group) == segments.end()) {
        segment = new segment_info;

        segment->name = generate_uuid();
        segment->group = group;
        segment->basename = segment->name + ".tar";
        segment->basename += filter_extension;
        segment->count = 0;
        segment->size = 0;
        segment->rf = remote->alloc_file(segment->basename, "segments");
        segment->file = new Tarfile(segment->rf, segment->name);

        segments[group] = segment;
    } else {
        segment = segments[group];
    }

    int id = segment->count;
    char id_buf[64];
    sprintf(id_buf, "%08x", id);

    segment->file->write_object(id, data, len);
    segment->count++;
    segment->size += len;

    group_sizes[group].first += len;

    ObjectReference ref(segment->name, id_buf);

    // If this segment meets or exceeds the size target, close it so that
    // future objects will go into a new segment.
    if (segment->file->size_estimate() >= SEGMENT_SIZE)
        close_segment(group);

    return ref;
}

void TarSegmentStore::sync()
{
    while (!segments.empty())
        close_segment(segments.begin()->first);
}

void TarSegmentStore::dump_stats()
{
    printf("Data written:\n");
    for (map<string, pair<int64_t, int64_t> >::iterator i = group_sizes.begin();
         i != group_sizes.end(); ++i) {
        printf("    %s: %lld (%lld compressed)\n", i->first.c_str(),
               (long long)i->second.first, (long long)i->second.second);
    }
}

void TarSegmentStore::close_segment(const string &group)
{
    struct segment_info *segment = segments[group];

    delete segment->file;

    if (db != NULL) {
        SHA1Checksum segment_checksum;
        if (segment_checksum.process_file(segment->rf->get_local_path().c_str())) {
            string checksum = segment_checksum.checksum_str();
            db->SetSegmentChecksum(segment->name, segment->basename, checksum,
                                   segment->size);
        }

        struct stat stat_buf;
        if (stat(segment->rf->get_local_path().c_str(), &stat_buf) == 0) {
            group_sizes[segment->group].second += stat_buf.st_size;
        }
    }

    segment->rf->send();

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

void LbsObject::write(TarSegmentStore *store)
{
    assert(data != NULL);
    assert(!written);

    ref = store->write_object(data, data_len, group);
    written = true;
}

void LbsObject::checksum()
{
    assert(written);

    Hash *hash = Hash::New();
    hash->update(data, data_len);
    ref.set_checksum(hash->digest_str());
    delete hash;
}
