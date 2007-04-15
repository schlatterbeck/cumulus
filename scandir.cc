/* Recursively descend the filesystem and visit each file. */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <string>
#include <vector>
#include <iostream>
#include <sstream>

#include "store.h"
#include "tarstore.h"
#include "sha1.h"

using std::string;
using std::vector;
using std::ostream;

static SegmentStore *segment_store;
static OutputStream *info_dump = NULL;

static SegmentPartitioner *index_segment, *data_segment;

/* Buffer for holding a single block of data read from a file. */
static const int LBS_BLOCK_SIZE = 1024 * 1024;
static char *block_buf;

void scandir(const string& path, std::ostream& metadata);

/* Converts time to microseconds since the epoch. */
int64_t encode_time(time_t time)
{
    return (int64_t)time * 1000000;
}

/* Read data from a file descriptor and return the amount of data read.  A
 * short read (less than the requested size) will only occur if end-of-file is
 * hit. */
size_t file_read(int fd, char *buf, size_t maxlen)
{
    size_t bytes_read = 0;

    while (true) {
        ssize_t res = read(fd, buf, maxlen);
        if (res < 0) {
            if (errno == EINTR)
                continue;
            throw IOException("file_read: error reading");
        } else if (res == 0) {
            break;
        } else {
            bytes_read += res;
            buf += res;
            maxlen -= res;
        }
    }

    return bytes_read;
}

/* Read the contents of a file (specified by an open file descriptor) and copy
 * the data to the store. */
void dumpfile(int fd, dictionary &file_info)
{
    struct stat stat_buf;
    fstat(fd, &stat_buf);
    int64_t size = 0;

    if ((stat_buf.st_mode & S_IFMT) != S_IFREG) {
        printf("file is no longer a regular file!\n");
        return;
    }

    /* The index data consists of a sequence of pointers to the data blocks
     * that actually comprise the file data.  This level of indirection is used
     * so that the same data block can be used in multiple files, or multiple
     * versions of the same file. */
    struct uuid segment_uuid;
    int object_id;
    OutputStream *index_data = index_segment->new_object(&segment_uuid,
                                                         &object_id,
                                                         "DREF");

    SHA1Checksum hash;
    while (true) {
        struct uuid block_segment_uuid;
        int block_object_id;

        size_t bytes = file_read(fd, block_buf, LBS_BLOCK_SIZE);
        if (bytes == 0)
            break;

        hash.process(block_buf, bytes);
        OutputStream *block = data_segment->new_object(&block_segment_uuid,
                                                       &block_object_id,
                                                       "DATA");
        block->write(block_buf, bytes);
        index_data->write_uuid(block_segment_uuid);
        index_data->write_u32(block_object_id);

        size += bytes;
    }

    file_info["sha1"] = string((const char *)hash.checksum(),
                               hash.checksum_size());
    file_info["data"] = encode_objref(segment_uuid, object_id);
}

void scanfile(const string& path, ostream &metadata)
{
    int fd;
    long flags;
    struct stat stat_buf;
    char *buf;
    ssize_t len;

    // Set to true if the item is a directory and we should recursively scan
    bool recurse = false;

    dictionary file_info;

    lstat(path.c_str(), &stat_buf);

    printf("%s\n", path.c_str());

    metadata << "name: " << path << "\n";
    metadata << "mode: " << (stat_buf.st_mode & 07777) << "\n";
    metadata << "atime: " << stat_buf.st_atime << "\n";
    metadata << "ctime: " << stat_buf.st_ctime << "\n";
    metadata << "mtime: " << stat_buf.st_mtime << "\n";
    metadata << "user: " << stat_buf.st_uid << "\n";
    metadata << "group: " << stat_buf.st_gid << "\n";

    file_info["mode"] = encode_u16(stat_buf.st_mode & 07777);
    file_info["atime"] = encode_u64(encode_time(stat_buf.st_atime));
    file_info["ctime"] = encode_u64(encode_time(stat_buf.st_ctime));
    file_info["mtime"] = encode_u64(encode_time(stat_buf.st_mtime));
    file_info["user"] = encode_u32(stat_buf.st_uid);
    file_info["group"] = encode_u32(stat_buf.st_gid);

    char inode_type;

    switch (stat_buf.st_mode & S_IFMT) {
    case S_IFIFO:
        inode_type = 'p';
        break;
    case S_IFSOCK:
        inode_type = 's';
        break;
    case S_IFCHR:
        inode_type = 'c';
        break;
    case S_IFBLK:
        inode_type = 'b';
        break;
    case S_IFLNK:
        inode_type = 'l';

        /* Use the reported file size to allocate a buffer large enough to read
         * the symlink.  Allocate slightly more space, so that we ask for more
         * bytes than we expect and so check for truncation. */
        buf = new char[stat_buf.st_size + 2];
        len = readlink(path.c_str(), buf, stat_buf.st_size + 1);
        if (len < 0) {
            printf("error reading symlink: %m\n");
        } else if (len <= stat_buf.st_size) {
            buf[len] = '\0';
            printf("    contents=%s\n", buf);
        } else if (len > stat_buf.st_size) {
            printf("error reading symlink: name truncated\n");
        }

        file_info["contents"] = buf;

        delete[] buf;
        break;
    case S_IFREG:
        inode_type = '-';

        /* Be paranoid when opening the file.  We have no guarantee that the
         * file was not replaced between the stat() call above and the open()
         * call below, so we might not even be opening a regular file.  That
         * the file descriptor refers to a regular file is checked in
         * dumpfile().  But we also supply flags to open to to guard against
         * various conditions before we can perform that verification:
         *   - O_NOFOLLOW: in the event the file was replaced by a symlink
         *   - O_NONBLOCK: prevents open() from blocking if the file was
         *     replaced by a fifo
         * We also add in O_NOATIME, since this may reduce disk writes (for
         * inode updates). */
        fd = open(path.c_str(), O_RDONLY|O_NOATIME|O_NOFOLLOW|O_NONBLOCK);

        /* Drop the use of the O_NONBLOCK flag; we only wanted that for file
         * open. */
        flags = fcntl(fd, F_GETFL);
        fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);

        file_info["size"] = encode_u64(stat_buf.st_size);
        dumpfile(fd, file_info);
        close(fd);

        break;
    case S_IFDIR:
        inode_type = 'd';
        recurse = true;
        break;

    default:
        fprintf(stderr, "Unknown inode type: mode=%x\n", stat_buf.st_mode);
        return;
    }

    file_info["type"] = string(1, inode_type);
    metadata << "type: " << inode_type << "\n";

    info_dump->write_string(path);
    info_dump->write_dictionary(file_info);

    metadata << "\n";

    // If we hit a directory, now that we've written the directory itself,
    // recursively scan the directory.
    if (recurse)
        scandir(path, metadata);
}

void scandir(const string& path, ostream &metadata)
{
    DIR *dir = opendir(path.c_str());

    if (dir == NULL) {
        printf("Error: %m\n");
        return;
    }

    struct dirent *ent;
    vector<string> contents;
    while ((ent = readdir(dir)) != NULL) {
        string filename(ent->d_name);
        if (filename == "." || filename == "..")
            continue;
        contents.push_back(filename);
    }

    sort(contents.begin(), contents.end());

    for (vector<string>::iterator i = contents.begin();
         i != contents.end(); ++i) {
        const string& filename = *i;
        scanfile(path + "/" + filename, metadata);
    }

    closedir(dir);
}

int main(int argc, char *argv[])
{
    block_buf = new char[LBS_BLOCK_SIZE];

    segment_store = new SegmentStore(".");
    SegmentWriter *sw = segment_store->new_segment();
    info_dump = sw->new_object(NULL, "ROOT");

    index_segment = new SegmentPartitioner(segment_store);
    data_segment = new SegmentPartitioner(segment_store);

    string uuid = SegmentWriter::format_uuid(sw->get_uuid());
    printf("Backup UUID: %s\n", uuid.c_str());

    std::ostringstream metadata;

    try {
        scanfile(".", metadata);
    } catch (IOException e) {
        fprintf(stderr, "IOException: %s\n", e.getError().c_str());
    }

    Tarfile *t = new Tarfile("tarstore.tar", uuid);
    const char testdata[] = "Test string.";
    t->write_object(0, testdata, strlen(testdata));
    t->write_object(1, testdata, strlen(testdata));
    t->write_object(2, testdata, strlen(testdata));

    const string md = metadata.str();
    t->write_object(3, md.data(), md.size());

    delete t;

    delete index_segment;
    delete data_segment;
    delete sw;

    return 0;
}
