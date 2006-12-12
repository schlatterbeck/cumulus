/* Recursively descend the filesystem and visit each file. */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <string>
#include <vector>

using std::string;
using std::vector;

void scandir(const string& path);

void dumpfile(int fd)
{
    struct stat stat_buf;
    fstat(fd, &stat_buf);

    if ((stat_buf.st_mode & S_IFMT) != S_IFREG) {
        printf("file is no longer a regular file!\n");
        return;
    }
}

void scanfile(const string& path)
{
    int fd;
    long flags;
    struct stat stat_buf;
    lstat(path.c_str(), &stat_buf);

    printf("%s:\n", path.c_str());
    printf("  ino=%Ld, perm=%04o, uid=%d, gid=%d, nlink=%d, blksize=%d, size=%Ld\n",
           (int64_t)stat_buf.st_ino, stat_buf.st_mode & 07777,
           stat_buf.st_uid, stat_buf.st_gid, stat_buf.st_nlink,
           (int)stat_buf.st_blksize, (int64_t)stat_buf.st_size);

    switch (stat_buf.st_mode & S_IFMT) {
    case S_IFIFO:
    case S_IFSOCK:
    case S_IFCHR:
    case S_IFBLK:
    case S_IFLNK:
        printf("  special file\n");
        break;
    case S_IFREG:
        printf("  regular file\n");
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

        dumpfile(fd);
        close(fd);

        break;
    case S_IFDIR:
        printf("  directory\n");
        scandir(path);
        break;
    }
}

void scandir(const string& path)
{
    printf("Scan directory: %s\n", path.c_str());

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
        printf("  d_name = '%s'\n", filename.c_str());
        scanfile(path + "/" + filename);
    }

    closedir(dir);
}

int main(int argc, char *argv[])
{
    scandir(".");

    return 0;
}
