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

#include <assert.h>
#include <stdio.h>
#include <string.h>

#include <fstream>
#include <iostream>
#include <string>

#include "format.h"
#include "statcache.h"

using std::list;
using std::string;
using std::ifstream;
using std::ofstream;

void StatCache::Open(const char *path, const char *snapshot_name)
{
    oldpath = path;
    oldpath += "/statcache";
    newpath = oldpath + "." + snapshot_name;

    oldcache = NULL;
    newcache = new ofstream(newpath.c_str());
}

void StatCache::Close()
{
    if (oldcache != NULL)
        delete oldcache;

    delete newcache;

    if (rename(newpath.c_str(), oldpath.c_str()) < 0) {
        fprintf(stderr, "Error renaming statcache from %s to %s: %m\n",
                newpath.c_str(), oldpath.c_str());
    }
}

/* Save stat information about a regular file for future invocations. */
void StatCache::Save(const string &path, struct stat *stat_buf,
                     const string &checksum, const list<string> &blocks)
{
    *newcache << uri_encode(path) << "\n";
    *newcache << "mtime: " << encode_int(stat_buf->st_mtime) << "\n"
              << "ctime: " << encode_int(stat_buf->st_ctime) << "\n"
              << "inode: " << encode_int(stat_buf->st_ino) << "\n"
              << "checksum: " << checksum << "\n";

    *newcache << "blocks:";
    for (list<string>::const_iterator i = blocks.begin();
         i != blocks.end(); ++i) {
        *newcache << " " << *i << "\n";
    }

    *newcache << "\n";
}
