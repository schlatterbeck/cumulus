/* LBS: An LFS-inspired filesystem backup system
 * Copyright (C) 2007  Michael Vrable
 *
 * Backup data is stored in a collection of objects, which are grouped together
 * into segments for storage purposes.  This implementation of the object store
 * represents segments as TAR files and objects as files within them. */

#include <assert.h>
#include <errno.h>
#include <stdio.h>
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

#include "store.h"
#include "ref.h"

using std::max;
using std::list;
using std::map;
using std::set;
using std::string;

/* Default filter program is bzip2 */
const char *filter_program = "bzip2 -c";
const char *filter_extension = ".bz2";

static void cloexec(int fd)
{
    long flags = fcntl(fd, F_GETFD);

    if (flags < 0)
        return;

    fcntl(fd, F_SETFD, flags | FD_CLOEXEC);
}

Tarfile::Tarfile(const string &path, const string &segment)
    : size(0),
      segment_name(segment)
{
    assert(sizeof(struct tar_header) == TAR_BLOCK_SIZE);

    real_fd = open(path.c_str(), O_WRONLY | O_CREAT, 0666);
    if (real_fd < 0)
        throw IOException("Error opening output file");

    filter_fd = spawn_filter(real_fd);
}

Tarfile::~Tarfile()
{
    char buf[TAR_BLOCK_SIZE];

    /* Append the EOF marker: two blocks filled with nulls. */
    memset(buf, 0, sizeof(buf));
    tar_write(buf, TAR_BLOCK_SIZE);
    tar_write(buf, TAR_BLOCK_SIZE);

    if (close(filter_fd) != 0)
        throw IOException("Error closing Tarfile");

    /* ...and wait for filter process to finish. */
    int status;
    waitpid(filter_pid, &status, 0);

    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        throw IOException("Filter process error");
    }

    close(real_fd);
}

/* Launch a child process which can act as a filter (compress, encrypt, etc.)
 * on the TAR output.  The file descriptor to which output should be written
 * must be specified; the return value is the file descriptor which will be
 * attached to the standard input of the filter program. */
int Tarfile::spawn_filter(int fd_out)
{
    int fds[2];

    /* Create a pipe for communicating with the filter process. */
    if (pipe(fds) < 0) {
        throw IOException("Unable to create pipe for filter");
    }

    /* Create a child process which can exec() the filter program. */
    filter_pid = fork();
    if (filter_pid < 0)
        throw IOException("Unable to fork filter process");

    if (filter_pid > 0) {
        /* Parent process */
        close(fds[0]);
        cloexec(fds[1]);
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
        execlp("/bin/sh", "/bin/sh", "-c", filter_program, NULL);

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
            throw IOException("Write error");
        }

        len -= res;
        data += res;
    }
}

void Tarfile::write_object(int id, const char *data, size_t len)
{
    char buf[64];
    sprintf(buf, "%08x", id);
    string path = segment_name + "/" + buf;

    internal_write_object(path, data, len);
}

void Tarfile::internal_write_object(const string &path,
                                    const char *data, size_t len)
{
    struct tar_header header;
    memset(&header, 0, sizeof(header));

    assert(path.size() < 100);
    memcpy(header.name, path.data(), path.size());
    sprintf(header.mode, "%07o", 0600);
    sprintf(header.uid, "%07o", 0);
    sprintf(header.gid, "%07o", 0);
    sprintf(header.size, "%011o", len);
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

static map<string, int64_t> group_sizes;

ObjectReference TarSegmentStore::write_object(const char *data, size_t len,
                                              const std::string &group)
{
    struct segment_info *segment;

    // Find the segment into which the object should be written, looking up by
    // group.  If no segment exists yet, create one.
    if (segments.find(group) == segments.end()) {
        segment = new segment_info;

        segment->name = generate_uuid();

        string filename = path + "/" + segment->name + ".tar";
        filename += filter_extension;
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

    group_sizes[group] += len;

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
    for (map<string, int64_t>::iterator i = group_sizes.begin();
         i != group_sizes.end(); ++i) {
        printf("    %s: %lld\n", i->first.c_str(), i->second);
    }
}

void TarSegmentStore::close_segment(const string &group)
{
    struct segment_info *segment = segments[group];

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

    SHA1Checksum hash;
    hash.process(data, data_len);
    ref.set_checksum(hash.checksum_str());
}
