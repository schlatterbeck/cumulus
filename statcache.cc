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
#include <ctype.h>

#include <fstream>
#include <iostream>
#include <map>
#include <string>

#include "format.h"
#include "ref.h"
#include "statcache.h"

using std::list;
using std::map;
using std::string;
using std::getline;
using std::ifstream;
using std::ofstream;

/* Like strcmp, but sorts in the order that files will be visited in the
 * filesystem.  That is, we break paths apart at slashes, and compare path
 * components separately. */
static int pathcmp(const char *path1, const char *path2)
{
    /* Find the first component in each path. */
    const char *slash1 = strchr(path1, '/');
    const char *slash2 = strchr(path2, '/');

    {
        string comp1, comp2;
        if (slash1 == NULL)
            comp1 = path1;
        else
            comp1 = string(path1, slash1 - path1);

        if (slash2 == NULL)
            comp2 = path2;
        else
            comp2 = string(path2, slash2 - path2);

        /* Directly compare the two components first. */
        if (comp1 < comp2)
            return -1;
        if (comp1 > comp2)
            return 1;
    }

    if (slash1 == NULL && slash2 == NULL)
        return 0;
    if (slash1 == NULL)
        return -1;
    if (slash2 == NULL)
        return 1;

    return pathcmp(slash1 + 1, slash2 + 1);
}

void StatCache::Open(const char *path, const char *snapshot_name)
{
    oldpath = path;
    oldpath += "/statcache";
    newpath = oldpath + "." + snapshot_name;

    oldcache = new ifstream(oldpath.c_str());
    newcache = new ofstream(newpath.c_str());

    /* Read the first entry from the old stat cache into memory before we
     * start. */
    ReadNext();
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

/* Read the next entry from the old statcache file and cache it in memory. */
void StatCache::ReadNext()
{
    if (oldcache == NULL) {
        end_of_cache = true;
        return;
    }

    std::istream &cache = *oldcache;
    map<string, string> fields;

    old_mtime = -1;
    old_ctime = -1;
    old_inode = -1;
    old_checksum = "";
    old_contents.clear();

    /* First, read in the filename. */
    getline(cache, old_name);
    if (!cache) {
        end_of_cache = true;
        return;
    }

    /* Start reading in the fields which follow the filename. */
    string field = "";
    while (!cache.eof()) {
        string line;
        getline(cache, line);
        const char *s = line.c_str();

        /* Is the line blank?  If so, we have reached the end of this entry. */
        if (s[0] == '\0' || s[0] == '\n')
            break;

        /* Is this a continuation line?  (Does it start with whitespace?) */
        if (isspace(s[0]) && field != "") {
            fields[field] += line;
            continue;
        }

        /* For lines of the form "Key: Value" look for ':' and split the line
         * apart. */
        const char *value = strchr(s, ':');
        if (value == NULL)
            continue;
        field = string(s, value - s);

        value++;
        while (isspace(*value))
            value++;

        fields[field] = value;
    }

    /* Parse the easy fields: mtime, ctime, inode, checksum, ... */
    if (fields.count("mtime"))
        old_mtime = parse_int(fields["mtime"]);
    if (fields.count("ctime"))
        old_ctime = parse_int(fields["ctime"]);
    if (fields.count("inode"))
        old_inode = parse_int(fields["inode"]);

    old_checksum = fields["checksum"];

    /* Parse the list of blocks. */
    const char *s = fields["blocks"].c_str();
    while (*s != '\0') {
        if (isspace(*s)) {
            s++;
            continue;
        }

        string ref = "";
        while (*s != '\0' && !isspace(*s)) {
            char buf[2];
            buf[0] = *s;
            buf[1] = '\0';
            ref += buf;
            s++;
        }

        ObjectReference *r = ObjectReference::parse(ref);
        if (r != NULL) {
            old_contents.push_back(*r);
            delete r;
        }
    }

    end_of_cache = false;
}

/* Find information about the given filename in the old stat cache, if it
 * exists. */
bool StatCache::Find(const string &path, const struct stat *stat_buf)
{
    while (!end_of_cache && pathcmp(old_name.c_str(), path.c_str()) < 0)
        ReadNext();

    /* Could the file be found at all? */
    if (end_of_cache)
        return false;
    if (old_name != path)
        return false;

    /* Check to see if the file is unchanged. */
    if (stat_buf->st_mtime != old_mtime)
        return false;
    if (stat_buf->st_ctime != old_ctime)
        return false;
    if ((long long)stat_buf->st_ino != old_inode)
        return false;

    /* File looks to be unchanged. */
    return true;
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
    if (blocks.size() == 0)
        *newcache << "\n";
    for (list<string>::const_iterator i = blocks.begin();
         i != blocks.end(); ++i) {
        *newcache << " " << *i << "\n";
    }

    *newcache << "\n";
}
