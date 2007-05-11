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
#include <list>
#include <vector>
#include <iostream>
#include <sstream>
#include <set>

#include "format.h"
#include "store.h"
#include "sha1.h"

using std::list;
using std::string;
using std::vector;
using std::ostream;

static TarSegmentStore *tss = NULL;

/* Buffer for holding a single block of data read from a file. */
static const int LBS_BLOCK_SIZE = 1024 * 1024;
static char *block_buf;

/* Contents of the root object.  This will contain a set of indirect links to
 * the metadata objects. */
std::ostringstream metadata_root;

/* Buffer for building up metadata. */
std::ostringstream metadata;

/* Keep track of all segments which are needed to reconstruct the snapshot. */
std::set<string> segment_list;

void scandir(const string& path);

/* Ensure contents of metadata are flushed to an object. */
void metadata_flush()
{
    string m = metadata.str();
    if (m.size() == 0)
        return;

    /* Write current metadata information to a new object. */
    LbsObject *meta = new LbsObject;
    meta->set_group("root");
    meta->set_data(m.data(), m.size());
    meta->write(tss);
    meta->checksum();

    /* Write a reference to this block in the root. */
    ObjectReference ref = meta->get_ref();
    metadata_root << "@" << ref.to_string() << "\n";
    segment_list.insert(ref.get_segment());

    delete meta;

    metadata.str("");
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
    list<string> object_list;

    if ((stat_buf.st_mode & S_IFMT) != S_IFREG) {
        printf("file is no longer a regular file!\n");
        return;
    }

    /* The index data consists of a sequence of pointers to the data blocks
     * that actually comprise the file data.  This level of indirection is used
     * so that the same data block can be used in multiple files, or multiple
     * versions of the same file. */
    SHA1Checksum hash;
    while (true) {
        size_t bytes = file_read(fd, block_buf, LBS_BLOCK_SIZE);
        if (bytes == 0)
            break;

        hash.process(block_buf, bytes);

        // tarstore processing
        LbsObject *o = new LbsObject;
        o->set_group("data");
        o->set_data(block_buf, bytes);
        o->write(tss);
        object_list.push_back(o->get_name());
        segment_list.insert(o->get_ref().get_segment());
        delete o;

        size += bytes;
    }

    file_info["checksum"] = hash.checksum_str();

    /* For files that only need to be broken apart into a few objects, store
     * the list of objects directly.  For larger files, store the data
     * out-of-line and provide a pointer to the indrect object. */
    if (object_list.size() < 8) {
        string blocklist = "";
        for (list<string>::iterator i = object_list.begin();
             i != object_list.end(); ++i) {
            if (i != object_list.begin())
                blocklist += " ";
            blocklist += *i;
        }
        file_info["data"] = blocklist;
    } else {
        string blocklist = "";
        for (list<string>::iterator i = object_list.begin();
             i != object_list.end(); ++i) {
            blocklist += *i + "\n";
        }

        LbsObject *i = new LbsObject;
        i->set_group("indirect");
        i->set_data(blocklist.data(), blocklist.size());
        i->write(tss);
        file_info["data"] = "@" + i->get_name();
        segment_list.insert(i->get_ref().get_segment());
        delete i;
    }
}

void scanfile(const string& path)
{
    int fd;
    long flags;
    struct stat stat_buf;
    char *buf;
    ssize_t len;
    list<string> refs;

    // Set to true if the item is a directory and we should recursively scan
    bool recurse = false;

    dictionary file_info;

    lstat(path.c_str(), &stat_buf);

    printf("%s\n", path.c_str());

    metadata << "name: " << uri_encode(path) << "\n";

    file_info["mode"] = encode_int(stat_buf.st_mode & 07777);
    file_info["atime"] = encode_int(stat_buf.st_atime);
    file_info["ctime"] = encode_int(stat_buf.st_ctime);
    file_info["mtime"] = encode_int(stat_buf.st_mtime);
    file_info["user"] = encode_int(stat_buf.st_uid);
    file_info["group"] = encode_int(stat_buf.st_gid);

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

        file_info["contents"] = uri_encode(buf);

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

        file_info["size"] = encode_int(stat_buf.st_size);
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

    dict_output(metadata, file_info);
    metadata << "\n";

    // Break apart metadata listing if it becomes too large.
    if (metadata.str().size() > 4096)
        metadata_flush();

    // If we hit a directory, now that we've written the directory itself,
    // recursively scan the directory.
    if (recurse)
        scandir(path);
}

void scandir(const string& path)
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
        scanfile(path + "/" + filename);
    }

    closedir(dir);
}

int main(int argc, char *argv[])
{
    block_buf = new char[LBS_BLOCK_SIZE];

    if (argc > 1) {
        tss = new TarSegmentStore(argv[1]);
    } else {
        tss = new TarSegmentStore(".");
    }

    try {
        scanfile(".");
    } catch (IOException e) {
        fprintf(stderr, "IOException: %s\n", e.getError().c_str());
    }

    metadata_flush();
    const string md = metadata_root.str();

    LbsObject *root = new LbsObject;
    root->set_group("root");
    root->set_data(md.data(), md.size());
    root->write(tss);
    root->checksum();

    segment_list.insert(root->get_ref().get_segment());
    string r = root->get_ref().to_string();
    printf("root: %s\n\n", r.c_str());
    delete root;

    printf("segments:\n");
    for (std::set<string>::iterator i = segment_list.begin();
         i != segment_list.end(); ++i) {
        printf("    %s\n", i->c_str());
    }

    tss->sync();
    delete tss;

    return 0;
}
