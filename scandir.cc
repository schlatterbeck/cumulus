/* Recursively descend the filesystem and visit each file. */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pwd.h>
#include <grp.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <string>
#include <list>
#include <vector>
#include <iostream>
#include <fstream>
#include <sstream>
#include <set>

#include "format.h"
#include "localdb.h"
#include "store.h"
#include "sha1.h"

using std::list;
using std::string;
using std::vector;
using std::ostream;

static TarSegmentStore *tss = NULL;

/* Buffer for holding a single block of data read from a file. */
static const size_t LBS_BLOCK_SIZE = 1024 * 1024;
static char *block_buf;

static const size_t LBS_METADATA_BLOCK_SIZE = 65536;

/* Local database, which tracks objects written in this and previous
 * invocations to help in creating incremental snapshots. */
LocalDb *db;

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
 * the data to the store.  Returns the size of the file (number of bytes
 * dumped), or -1 on error. */
int64_t dumpfile(int fd, dictionary &file_info)
{
    struct stat stat_buf;
    fstat(fd, &stat_buf);
    int64_t size = 0;
    list<string> object_list;

    if ((stat_buf.st_mode & S_IFMT) != S_IFREG) {
        fprintf(stderr, "file is no longer a regular file!\n");
        return -1;
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

        // Either find a copy of this block in an already-existing segment, or
        // index it so it can be re-used in the future
        SHA1Checksum block_hash;
        block_hash.process(block_buf, bytes);
        string block_csum = block_hash.checksum_str();
        ObjectReference ref = db->FindObject(block_csum, bytes);

        // Store a copy of the object if one does not yet exist
        if (ref.get_segment().size() == 0) {
            LbsObject *o = new LbsObject;
            o->set_group("data");
            o->set_data(block_buf, bytes);
            o->write(tss);
            ref = o->get_ref();
            db->StoreObject(ref, block_csum, bytes);
            delete o;
        }
        object_list.push_back(ref.to_string());
        segment_list.insert(ref.get_segment());
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

    return size;
}

void scanfile(const string& path)
{
    int fd;
    long flags;
    struct stat stat_buf;
    char *buf;
    ssize_t len;
    int64_t file_size;
    list<string> refs;

    // Set to true if the item is a directory and we should recursively scan
    bool recurse = false;

    dictionary file_info;

    lstat(path.c_str(), &stat_buf);

    printf("%s\n", path.c_str());

    metadata << "name: " << uri_encode(path) << "\n";

    file_info["mode"] = encode_int(stat_buf.st_mode & 07777);
    file_info["mtime"] = encode_int(stat_buf.st_mtime);
    file_info["user"] = encode_int(stat_buf.st_uid);
    file_info["group"] = encode_int(stat_buf.st_gid);

    struct passwd *pwd = getpwuid(stat_buf.st_uid);
    if (pwd != NULL) {
        file_info["user"] += " (" + uri_encode(pwd->pw_name) + ")";
    }

    struct group *grp = getgrgid(stat_buf.st_gid);
    if (pwd != NULL) {
        file_info["group"] += " (" + uri_encode(grp->gr_name) + ")";
    }

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
            fprintf(stderr, "error reading symlink: %m\n");
        } else if (len <= stat_buf.st_size) {
            buf[len] = '\0';
            file_info["contents"] = uri_encode(buf);
        } else if (len > stat_buf.st_size) {
            fprintf(stderr, "error reading symlink: name truncated\n");
        }

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

        file_size = dumpfile(fd, file_info);
        file_info["size"] = encode_int(file_size);
        close(fd);

        if (file_size < 0)
            return;             // error occurred; do not dump file

        if (file_size != stat_buf.st_size) {
            fprintf(stderr, "Warning: Size of %s changed during reading\n",
                    path.c_str());
        }

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

    dict_output(metadata, file_info);
    metadata << "\n";

    // Break apart metadata listing if it becomes too large.
    if (metadata.str().size() > LBS_METADATA_BLOCK_SIZE)
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
        fprintf(stderr, "Error: %m\n");
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

    string backup_dest = ".";

    if (argc > 1)
        backup_dest = argv[1];

    tss = new TarSegmentStore(backup_dest);

    string database_path = backup_dest + "/localdb.sqlite";
    db = new LocalDb;
    db->Open(database_path.c_str());

    /* Write a backup descriptor file, which says which segments are needed and
     * where to start to restore this snapshot.  The filename is based on the
     * current time. */
    time_t now;
    struct tm time_buf;
    char desc_buf[256];
    time(&now);
    localtime_r(&now, &time_buf);
    strftime(desc_buf, sizeof(desc_buf), "%Y%m%dT%H%M%S", &time_buf);
    string desc_filename = backup_dest + "/" + desc_buf + ".lbs";
    std::ofstream descriptor(desc_filename.c_str());

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
    descriptor << "Root: " << root->get_ref().to_string() << "\n";
    strftime(desc_buf, sizeof(desc_buf), "%Y-%m-%d %H:%M:%S %z", &time_buf);
    descriptor << "Date: " << desc_buf << "\n";

    delete root;

    descriptor << "Segments:\n";
    for (std::set<string>::iterator i = segment_list.begin();
         i != segment_list.end(); ++i) {
        descriptor << "    " << *i << "\n";
    }

    db->Close();

    tss->sync();
    delete tss;

    return 0;
}
