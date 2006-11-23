/* Recursively descend the filesystem and visit each file. */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <string>

using std::string;

void scandir(const string& path);

void scanfile(const string& path)
{
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
    while ((ent = readdir(dir)) != NULL) {
        string filename(ent->d_name);
        if (filename == "." || filename == "..")
            continue;
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
