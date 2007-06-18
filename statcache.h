/* LBS: An LFS-inspired filesystem backup system Copyright (C) 2007  Michael
 * Vrable
 *
 * To speed backups, we maintain a "stat cache" containing selected information
 * about all regular files, including modification times and the list of blocks
 * that comprised the file in the last backup.  If the file has not changed
 * according to a stat() call, we may re-use the information contained in the
 * stat cache instead of re-reading the entire file.  It is always safe to
 * discard information from the stat cache; this will only cause a file to be
 * re-read to determine that it contains the same data as before.
 *
 * The stat cache is stored in a file called "statcache" in the local backup
 * directory.  During a backup, a new statcache file is written out with a
 * suffix based on the current time; at the end of a successful backup this
 * file is renamed over the original statcache file.
 *
 * The information in the statcache file is stored in sorted order as we
 * traverse the filesystem, so that we can read and write it in a purely
 * streaming manner.  (This is why we don't include the information in the
 * SQLite local database; doing so is likely less efficient.)
 */

#ifndef _LBS_STATCACHE_H
#define _LBS_STATCACHE_H

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <iostream>
#include <list>
#include <string>

class StatCache {
public:
    void Open(const char *path, const char *snapshot_name);
    void Close();
    void Save(const std::string &path, struct stat *stat_buf,
              const std::string &checksum,
              const std::list<std::string> &blocks);

private:
    std::string oldpath, newpath;
    std::ifstream *oldcache;
    std::ofstream *newcache;
};

#endif // _LBS_STATCACHE_H
