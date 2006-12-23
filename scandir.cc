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

#include "store.h"

using std::string;
using std::vector;

static OutputStream *info_dump = NULL;

void scandir(const string& path);

/* Converts time to microseconds since the epoch. */
int64_t encode_time(time_t time)
{
    return (int64_t)time * 1000000;
}

void dumpfile(int fd)
{
    struct stat stat_buf;
    fstat(fd, &stat_buf);
    int64_t size = 0;

    char buf[4096];

    if ((stat_buf.st_mode & S_IFMT) != S_IFREG) {
        printf("file is no longer a regular file!\n");
        return;
    }

    while (true) {
        ssize_t res = read(fd, buf, sizeof(buf));
        if (res < 0) {
            if (errno == EINTR)
                continue;
            printf("Error while reading: %m\n");
            return;
        } else if (res == 0) {
            break;
        } else {
            size += res;
        }
    }

    printf("    bytes=%Ld\n", size);
}

void scanfile(const string& path)
{
    int fd;
    long flags;
    struct stat stat_buf;
    char *buf;
    ssize_t len;

    dictionary file_info;

    lstat(path.c_str(), &stat_buf);

    printf("%s\n", path.c_str());

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

        //dumpfile(fd);
        file_info["size"] = encode_u64(stat_buf.st_size);
        close(fd);

        break;
    case S_IFDIR:
        inode_type = 'd';
        scandir(path);
        break;

    default:
        fprintf(stderr, "Unknown inode type: mode=%x\n", stat_buf.st_mode);
        return;
    }

    file_info["type"] = string(1, inode_type);

    info_dump->write_string(path);
    info_dump->write_dictionary(file_info);
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
    FILE *dump = fopen("fileinfo", "w");
    if (dump == NULL) {
        fprintf(stderr, "Cannot open fileinfo: %m\n");
        return 1;
    }

    FileOutputStream os(dump);
    info_dump = &os;

    try {
        scanfile(".");
    } catch (IOException e) {
        fprintf(stderr, "IOException: %s\n", e.getError().c_str());
    }

    return 0;
}
