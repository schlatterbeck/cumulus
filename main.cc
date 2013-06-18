/* Cumulus: Efficient Filesystem Backup to the Cloud
 * Copyright (C) 2006-2009, 2012 The Cumulus Developers
 * See the AUTHORS file for a list of contributors.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

/* Main entry point for Cumulus.  Contains logic for traversing the filesystem
 * and constructing a backup. */

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <grp.h>
#include <pwd.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <list>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "exclude.h"
#include "hash.h"
#include "localdb.h"
#include "metadata.h"
#include "remote.h"
#include "store.h"
#include "subfile.h"
#include "util.h"
#include "third_party/sha1.h"

using std::list;
using std::map;
using std::string;
using std::vector;
using std::ostream;

/* Version information.  This will be filled in by the Makefile. */
#ifndef CUMULUS_VERSION
#define CUMULUS_VERSION Unknown
#endif
#define CUMULUS_STRINGIFY(s) CUMULUS_STRINGIFY2(s)
#define CUMULUS_STRINGIFY2(s) #s
static const char cumulus_version[] = CUMULUS_STRINGIFY(CUMULUS_VERSION);

static RemoteStore *remote = NULL;
static TarSegmentStore *tss = NULL;
static MetadataWriter *metawriter = NULL;

/* Buffer for holding a single block of data read from a file. */
static const size_t LBS_BLOCK_SIZE = 1024 * 1024;
static char *block_buf;

/* Local database, which tracks objects written in this and previous
 * invocations to help in creating incremental snapshots. */
LocalDb *db;

/* Selection of files to include/exclude in the snapshot. */
PathFilterList filter_rules;

bool flag_rebuild_statcache = false;

/* Whether verbose output is enabled. */
bool verbose = false;

/* Attempts to open a regular file read-only, but with safety checks for files
 * that might not be fully trusted. */
int safe_open(const string& path, struct stat *stat_buf)
{
    int fd;

    /* Be paranoid when opening the file.  We have no guarantee that the
     * file was not replaced between the stat() call above and the open()
     * call below, so we might not even be opening a regular file.  We
     * supply flags to open to to guard against various conditions before
     * we can perform an lstat to check that the file is still a regular
     * file:
     *   - O_NOFOLLOW: in the event the file was replaced by a symlink
     *   - O_NONBLOCK: prevents open() from blocking if the file was
     *     replaced by a fifo
     * We also add in O_NOATIME, since this may reduce disk writes (for
     * inode updates).  However, O_NOATIME may result in EPERM, so if the
     * initial open fails, try again without O_NOATIME.  */
    fd = open(path.c_str(), O_RDONLY|O_NOATIME|O_NOFOLLOW|O_NONBLOCK);
    if (fd < 0) {
        fd = open(path.c_str(), O_RDONLY|O_NOFOLLOW|O_NONBLOCK);
    }
    if (fd < 0) {
        fprintf(stderr, "Unable to open file %s: %m\n", path.c_str());
        return -1;
    }

    /* Drop the use of the O_NONBLOCK flag; we only wanted that for file
     * open. */
    long flags = fcntl(fd, F_GETFL);
    fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);

    /* Re-check file attributes, storing them into stat_buf if that is
     * non-NULL. */
    struct stat internal_stat_buf;
    if (stat_buf == NULL)
        stat_buf = &internal_stat_buf;

    /* Perform the stat call again, and check that we still have a regular
     * file. */
    if (fstat(fd, stat_buf) < 0) {
        fprintf(stderr, "fstat: %m\n");
        close(fd);
        return -1;
    }

    if ((stat_buf->st_mode & S_IFMT) != S_IFREG) {
        fprintf(stderr, "file is no longer a regular file!\n");
        close(fd);
        return -1;
    }

    return fd;
}

/* Read data from a file descriptor and return the amount of data read.  A
 * short read (less than the requested size) will only occur if end-of-file is
 * hit. */
ssize_t file_read(int fd, char *buf, size_t maxlen)
{
    size_t bytes_read = 0;

    while (true) {
        ssize_t res = read(fd, buf, maxlen);
        if (res < 0) {
            if (errno == EINTR)
                continue;
            fprintf(stderr, "error reading file: %m\n");
            return -1;
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
int64_t dumpfile(int fd, dictionary &file_info, const string &path,
                 struct stat& stat_buf)
{
    int64_t size = 0;
    list<string> object_list;
    const char *status = NULL;          /* Status indicator printed out */

    /* Look up this file in the old stat cache, if we can.  If the stat
     * information indicates that the file has not changed, do not bother
     * re-reading the entire contents.  Even if the information has been
     * changed, we can use the list of old blocks in the search for a sub-block
     * incremental representation. */
    bool cached = false;
    list<ObjectReference> old_blocks;

    bool found = metawriter->find(path);
    if (found)
        old_blocks = metawriter->get_blocks();

    if (found
        && !flag_rebuild_statcache
        && metawriter->is_unchanged(&stat_buf)) {
        cached = true;

        /* If any of the blocks in the object have been expired, then we should
         * fall back to fully reading in the file. */
        for (list<ObjectReference>::const_iterator i = old_blocks.begin();
             i != old_blocks.end(); ++i) {
            const ObjectReference &ref = *i;
            if (!db->IsAvailable(ref)) {
                cached = false;
                status = "repack";
                break;
            }
        }

        /* If everything looks okay, use the cached information */
        if (cached) {
            file_info["checksum"] = metawriter->get_checksum();
            for (list<ObjectReference>::const_iterator i = old_blocks.begin();
                 i != old_blocks.end(); ++i) {
                const ObjectReference &ref = *i;
                object_list.push_back(ref.to_string());
                db->UseObject(ref);
            }
            size = stat_buf.st_size;
        }
    }

    /* If the file is new or changed, we must read in the contents a block at a
     * time. */
    if (!cached) {
        Hash *hash = Hash::New();
        Subfile subfile(db);
        subfile.load_old_blocks(old_blocks);

        while (true) {
            ssize_t bytes = file_read(fd, block_buf, LBS_BLOCK_SIZE);
            if (bytes == 0)
                break;
            if (bytes < 0) {
                fprintf(stderr, "Backup contents for %s may be incorrect\n",
                        path.c_str());
                break;
            }

            hash->update(block_buf, bytes);

            // Sparse file processing: if we read a block of all zeroes, encode
            // that explicitly.
            bool all_zero = true;
            for (int i = 0; i < bytes; i++) {
                if (block_buf[i] != 0) {
                    all_zero = false;
                    break;
                }
            }

            // Either find a copy of this block in an already-existing segment,
            // or index it so it can be re-used in the future
            double block_age = 0.0;
            ObjectReference ref;

            Hash *hash = Hash::New();
            hash->update(block_buf, bytes);
            string block_csum = hash->digest_str();
            delete hash;

            if (all_zero) {
                ref = ObjectReference(ObjectReference::REF_ZERO);
                ref.set_range(0, bytes);
            } else {
                ref = db->FindObject(block_csum, bytes);
            }

            list<ObjectReference> refs;

            // Store a copy of the object if one does not yet exist
            if (ref.is_null()) {
                LbsObject *o = new LbsObject;
                int object_group;

                /* We might still have seen this checksum before, if the object
                 * was stored at some time in the past, but we have decided to
                 * clean the segment the object was originally stored in
                 * (FindObject will not return such objects).  When rewriting
                 * the object contents, put it in a separate group, so that old
                 * objects get grouped together.  The hope is that these old
                 * objects will continue to be used in the future, and we
                 * obtain segments which will continue to be well-utilized.
                 * Additionally, keep track of the age of the data by looking
                 * up the age of the block which was expired and using that
                 * instead of the current time. */
                if (db->IsOldObject(block_csum, bytes,
                                    &block_age, &object_group)) {
                    if (object_group == 0) {
                        o->set_group("data");
                    } else {
                        o->set_group(string_printf("compacted-%d",
                                                   object_group));
                    }
                    if (status == NULL)
                        status = "partial";
                } else {
                    o->set_group("data");
                    status = "new";
                }

                subfile.analyze_new_block(block_buf, bytes);
                refs = subfile.create_incremental(tss, o, block_age);
            } else {
                if (flag_rebuild_statcache && ref.is_normal()) {
                    subfile.analyze_new_block(block_buf, bytes);
                    subfile.store_analyzed_signatures(ref);
                }
                refs.push_back(ref);
            }

            while (!refs.empty()) {
                ref = refs.front(); refs.pop_front();
                object_list.push_back(ref.to_string());
                db->UseObject(ref);
            }
            size += bytes;

            if (status == NULL)
                status = "old";
        }

        file_info["checksum"] = hash->digest_str();
        delete hash;
    }

    // Sanity check: if we are rebuilding the statcache, but the file looks
    // like it hasn't changed, then the newly-computed checksum should match
    // the checksum in the statcache.  If not, we have possible disk corruption
    // and report a warning.
    if (flag_rebuild_statcache) {
        if (found
            && metawriter->is_unchanged(&stat_buf)
            && file_info["checksum"] != metawriter->get_checksum()) {
            fprintf(stderr,
                    "Warning: Checksum for %s does not match expected value\n"
                    "    expected: %s\n"
                    "    actual:   %s\n",
                    path.c_str(),
                    metawriter->get_checksum().c_str(),
                    file_info["checksum"].c_str());
        }
    }

    if (verbose && status != NULL)
        printf("    [%s]\n", status);

    string blocklist = "";
    for (list<string>::iterator i = object_list.begin();
         i != object_list.end(); ++i) {
        if (i != object_list.begin())
            blocklist += "\n    ";
        blocklist += *i;
    }
    file_info["data"] = blocklist;

    return size;
}

/* Look up a user/group and convert it to string form (either strictly numeric
 * or numeric plus symbolic).  Caches the results of the call to
 * getpwuid/getgrgid. */
string user_to_string(uid_t uid) {
    static map<uid_t, string> user_cache;
    map<uid_t, string>::const_iterator i = user_cache.find(uid);
    if (i != user_cache.end())
        return i->second;

    string result = encode_int(uid);
    struct passwd *pwd = getpwuid(uid);
    if (pwd != NULL && pwd->pw_name != NULL) {
        result += " (" + uri_encode(pwd->pw_name) + ")";
    }
    user_cache[uid] = result;
    return result;
}

string group_to_string(gid_t gid) {
    static map<gid_t, string> group_cache;
    map<gid_t, string>::const_iterator i = group_cache.find(gid);
    if (i != group_cache.end())
        return i->second;

    string result = encode_int(gid);
    struct group *grp = getgrgid(gid);
    if (grp != NULL && grp->gr_name != NULL) {
        result += " (" + uri_encode(grp->gr_name) + ")";
    }
    group_cache[gid] = result;
    return result;
}

/* Dump a specified filesystem object (file, directory, etc.) based on its
 * inode information.  If the object is a regular file, an open filehandle is
 * provided. */
void dump_inode(const string& path,         // Path within snapshot
                const string& fullpath,     // Path to object in filesystem
                struct stat& stat_buf,      // Results of stat() call
                int fd)                     // Open filehandle if regular file
{
    char *buf;
    dictionary file_info;
    int64_t file_size;
    ssize_t len;

    if (verbose)
        printf("%s\n", path.c_str());
    metawriter->find(path);

    file_info["name"] = uri_encode(path);
    file_info["mode"] = encode_int(stat_buf.st_mode & 07777, 8);
    file_info["ctime"] = encode_int(stat_buf.st_ctime);
    file_info["mtime"] = encode_int(stat_buf.st_mtime);
    file_info["user"] = user_to_string(stat_buf.st_uid);
    file_info["group"] = group_to_string(stat_buf.st_gid);

    time_t now = time(NULL);
    if (now - stat_buf.st_ctime < 30 || now - stat_buf.st_mtime < 30)
        if ((stat_buf.st_mode & S_IFMT) != S_IFDIR)
            file_info["volatile"] = "1";

    if (stat_buf.st_nlink > 1 && (stat_buf.st_mode & S_IFMT) != S_IFDIR) {
        file_info["links"] = encode_int(stat_buf.st_nlink);
    }

    file_info["inode"] = encode_int(major(stat_buf.st_dev))
        + "/" + encode_int(minor(stat_buf.st_dev))
        + "/" + encode_int(stat_buf.st_ino);

    char inode_type;

    switch (stat_buf.st_mode & S_IFMT) {
    case S_IFIFO:
        inode_type = 'p';
        break;
    case S_IFSOCK:
        inode_type = 's';
        break;
    case S_IFBLK:
    case S_IFCHR:
        inode_type = ((stat_buf.st_mode & S_IFMT) == S_IFBLK) ? 'b' : 'c';
        file_info["device"] = encode_int(major(stat_buf.st_rdev))
            + "/" + encode_int(minor(stat_buf.st_rdev));
        break;
    case S_IFLNK:
        inode_type = 'l';

        /* Use the reported file size to allocate a buffer large enough to read
         * the symlink.  Allocate slightly more space, so that we ask for more
         * bytes than we expect and so check for truncation. */
        buf = new char[stat_buf.st_size + 2];
        len = readlink(fullpath.c_str(), buf, stat_buf.st_size + 1);
        if (len < 0) {
            fprintf(stderr, "error reading symlink: %m\n");
        } else if (len <= stat_buf.st_size) {
            buf[len] = '\0';
            file_info["target"] = uri_encode(buf);
        } else if (len > stat_buf.st_size) {
            fprintf(stderr, "error reading symlink: name truncated\n");
        }

        delete[] buf;
        break;
    case S_IFREG:
        inode_type = 'f';

        file_size = dumpfile(fd, file_info, path, stat_buf);
        file_info["size"] = encode_int(file_size);

        if (file_size < 0)
            return;             // error occurred; do not dump file

        if (file_size != stat_buf.st_size) {
            fprintf(stderr, "Warning: Size of %s changed during reading\n",
                    path.c_str());
            file_info["volatile"] = "1";
        }

        break;
    case S_IFDIR:
        inode_type = 'd';
        break;

    default:
        fprintf(stderr, "Unknown inode type: mode=%x\n", stat_buf.st_mode);
        return;
    }

    file_info["type"] = string(1, inode_type);

    metawriter->add(file_info);
}

/* Converts a path to the normalized form used in the metadata log.  Paths are
 * written as relative (without any leading slashes).  The root directory is
 * referred to as ".". */
string metafile_path(const string& path)
{
    const char *newpath = path.c_str();
    if (*newpath == '/')
        newpath++;
    if (*newpath == '\0')
        newpath = ".";
    return newpath;
}

void try_merge_filter(const string& path, const string& basedir)
{
    struct stat stat_buf;
    if (lstat(path.c_str(), &stat_buf) < 0)
        return;
    if ((stat_buf.st_mode & S_IFMT) != S_IFREG)
        return;
    int fd = safe_open(path, NULL);
    if (fd < 0)
        return;

    /* As a very crude limit on the complexity of merge rules, only read up to
     * one block (1 MB) worth of data.  If the file doesn't seems like it might
     * be larger than that, don't parse the rules in it. */
    ssize_t bytes = file_read(fd, block_buf, LBS_BLOCK_SIZE);
    close(fd);
    if (bytes < 0 || bytes >= static_cast<ssize_t>(LBS_BLOCK_SIZE - 1)) {
        /* TODO: Add more strict resource limits on merge files? */
        fprintf(stderr,
                "Unable to read filter merge file (possibly size too large\n");
        return;
    }
    filter_rules.merge_patterns(metafile_path(path), basedir,
                                string(block_buf, bytes));
}

void scanfile(const string& path)
{
    int fd = -1;
    struct stat stat_buf;
    list<string> refs;

    string output_path = metafile_path(path);

    if (lstat(path.c_str(), &stat_buf) < 0) {
        fprintf(stderr, "lstat(%s): %m\n", path.c_str());
        return;
    }

    bool is_directory = ((stat_buf.st_mode & S_IFMT) == S_IFDIR);
    if (!filter_rules.is_included(output_path, is_directory))
        return;

    if ((stat_buf.st_mode & S_IFMT) == S_IFREG) {
        fd = safe_open(path, &stat_buf);
        if (fd < 0)
            return;
    }

    dump_inode(output_path, path, stat_buf, fd);

    if (fd >= 0)
        close(fd);

    /* If we hit a directory, now that we've written the directory itself,
     * recursively scan the directory. */
    if (is_directory) {
        DIR *dir = opendir(path.c_str());

        if (dir == NULL) {
            fprintf(stderr, "Error reading directory %s: %m\n",
                    path.c_str());
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

        closedir(dir);

        sort(contents.begin(), contents.end());

        filter_rules.save();

        /* First pass through the directory items: look for any filter rules to
         * merge and do so. */
        for (vector<string>::iterator i = contents.begin();
             i != contents.end(); ++i) {
            string filename;
            if (path == ".")
                filename = *i;
            else if (path == "/")
                filename = "/" + *i;
            else
                filename = path + "/" + *i;
            if (filter_rules.is_mergefile(metafile_path(filename))) {
                if (verbose) {
                    printf("Merging directory filter rules %s\n",
                           filename.c_str());
                }
                try_merge_filter(filename, output_path);
            }
        }

        /* Second pass: recursively scan all items in the directory for backup;
         * scanfile() will check if the item should be included or not. */
        for (vector<string>::iterator i = contents.begin();
             i != contents.end(); ++i) {
            const string& filename = *i;
            if (path == ".")
                scanfile(filename);
            else if (path == "/")
                scanfile("/" + filename);
            else
                scanfile(path + "/" + filename);
        }

        filter_rules.restore();
    }
}

void usage(const char *program)
{
    fprintf(
        stderr,
        "Cumulus %s\n\n"
        "Usage: %s [OPTION]... --dest=DEST PATHS...\n"
        "Produce backup snapshot of files in SOURCE and store to DEST.\n"
        "\n"
        "Options:\n"
        "  --dest=PATH          path where backup is to be written\n"
        "  --upload-script=COMMAND\n"
        "                       program to invoke for each backup file generated\n"
        "  --exclude=PATTERN    exclude files matching PATTERN from snapshot\n"
        "  --include=PATTERN    include files matching PATTERN in snapshot\n"
        "  --dir-merge=PATTERN  parse files matching PATTERN to read additional\n"
        "                       subtree-specific include/exclude rules during backup\n"
        "  --localdb=PATH       local backup metadata is stored in PATH\n"
        "  --tmpdir=PATH        path for temporarily storing backup files\n"
        "                           (defaults to TMPDIR environment variable or /tmp)\n"
        "  --filter=COMMAND     program through which to filter segment data\n"
        "                           (defaults to \"bzip2 -c\")\n"
        "  --filter-extension=EXT\n"
        "                       string to append to segment files\n"
        "                           (defaults to \".bz2\")\n"
        "  --signature-filter=COMMAND\n"
        "                       program though which to filter descriptor\n"
        "  --scheme=NAME        optional name for this snapshot\n"
        "  --intent=FLOAT       DEPRECATED: ignored, and will be removed soon\n"
        "  --full-metadata      do not re-use metadata from previous backups\n"
        "  --rebuild-statcache  re-read all file data to verify statcache\n"
        "  -v --verbose         list files as they are backed up\n"
        "\n"
        "Exactly one of --dest or --upload-script must be specified.\n",
        cumulus_version, program
    );
}

int main(int argc, char *argv[])
{
    hash_init();

    string backup_dest = "", backup_script = "";
    string localdb_dir = "";
    string backup_scheme = "";
    string signature_filter = "";

    string tmp_dir = "/tmp";
    if (getenv("TMPDIR") != NULL)
        tmp_dir = getenv("TMPDIR");

    while (1) {
        static struct option long_options[] = {
            {"localdb", 1, 0, 0},           // 0
            {"filter", 1, 0, 0},            // 1
            {"filter-extension", 1, 0, 0},  // 2
            {"dest", 1, 0, 0},              // 3
            {"scheme", 1, 0, 0},            // 4
            {"signature-filter", 1, 0, 0},  // 5
            {"intent", 1, 0, 0},            // 6, DEPRECATED
            {"full-metadata", 0, 0, 0},     // 7
            {"tmpdir", 1, 0, 0},            // 8
            {"upload-script", 1, 0, 0},     // 9
            {"rebuild-statcache", 0, 0, 0}, // 10
            {"include", 1, 0, 0},           // 11
            {"exclude", 1, 0, 0},           // 12
            {"dir-merge", 1, 0, 0},         // 13
            // Aliases for short options
            {"verbose", 0, 0, 'v'},
            {NULL, 0, 0, 0},
        };

        int long_index;
        int c = getopt_long(argc, argv, "v", long_options, &long_index);

        if (c == -1)
            break;

        if (c == 0) {
            switch (long_index) {
            case 0:     // --localdb
                localdb_dir = optarg;
                break;
            case 1:     // --filter
                filter_program = optarg;
                break;
            case 2:     // --filter-extension
                filter_extension = optarg;
                break;
            case 3:     // --dest
                backup_dest = optarg;
                break;
            case 4:     // --scheme
                backup_scheme = optarg;
                break;
            case 5:     // --signature-filter
                signature_filter = optarg;
                break;
            case 6:     // --intent
                fprintf(stderr,
                        "Warning: The --intent= option is deprecated and will "
                        "be removed in the future.\n");
                break;
            case 7:     // --full-metadata
                flag_full_metadata = true;
                break;
            case 8:     // --tmpdir
                tmp_dir = optarg;
                break;
            case 9:     // --upload-script
                backup_script = optarg;
                break;
            case 10:    // --rebuild-statcache
                flag_rebuild_statcache = true;
                break;
            case 11:    // --include
                filter_rules.add_pattern(PathFilterList::INCLUDE, optarg, "");
                break;
            case 12:    // --exclude
                filter_rules.add_pattern(PathFilterList::EXCLUDE, optarg, "");
                break;
            case 13:    // --dir-merge
                filter_rules.add_pattern(PathFilterList::DIRMERGE, optarg, "");
                break;
            default:
                fprintf(stderr, "Unhandled long option!\n");
                return 1;
            }
        } else {
            switch (c) {
            case 'v':
                verbose = true;
                break;
            default:
                usage(argv[0]);
                return 1;
            }
        }
    }

    if (optind == argc) {
        usage(argv[0]);
        return 1;
    }

    if (backup_dest == "" && backup_script == "") {
        fprintf(stderr,
                "Error: Backup destination must be specified using --dest= or --upload-script=\n");
        usage(argv[0]);
        return 1;
    }

    if (backup_dest != "" && backup_script != "") {
        fprintf(stderr,
                "Error: Cannot specify both --dest= and --upload-script=\n");
        usage(argv[0]);
        return 1;
    }

    // Default for --localdb is the same as --dest
    if (localdb_dir == "") {
        localdb_dir = backup_dest;
    }
    if (localdb_dir == "") {
        fprintf(stderr,
                "Error: Must specify local database path with --localdb=\n");
        usage(argv[0]);
        return 1;
    }

    block_buf = new char[LBS_BLOCK_SIZE];

    /* Initialize the remote storage layer.  If using an upload script, create
     * a temporary directory for staging files.  Otherwise, write backups
     * directly to the destination directory. */
    if (backup_script != "") {
        tmp_dir = tmp_dir + "/cumulus." + generate_uuid();
        if (mkdir(tmp_dir.c_str(), 0700) < 0) {
            fprintf(stderr, "Cannot create temporary directory %s: %m\n",
                    tmp_dir.c_str());
            return 1;
        }
        remote = new RemoteStore(tmp_dir, backup_script=backup_script);
    } else {
        remote = new RemoteStore(backup_dest);
    }

    /* Store the time when the backup started, so it can be included in the
     * snapshot name. */
    time_t now;
    time(&now);
    string timestamp
        = TimeFormat::format(now, TimeFormat::FORMAT_FILENAME, true);

    /* Open the local database which tracks all objects that are stored
     * remotely, for efficient incrementals.  Provide it with the name of this
     * snapshot. */
    string database_path = localdb_dir + "/localdb.sqlite";
    db = new LocalDb;
    db->Open(database_path.c_str(), timestamp.c_str(), backup_scheme.c_str());

    tss = new TarSegmentStore(remote, db);

    /* Initialize the stat cache, for skipping over unchanged files. */
    metawriter = new MetadataWriter(tss, localdb_dir.c_str(), timestamp.c_str(),
                                    backup_scheme.c_str());

    for (int i = optind; i < argc; i++) {
        scanfile(argv[i]);
    }

    ObjectReference root_ref = metawriter->close();
    string backup_root = root_ref.to_string();

    delete metawriter;

    tss->sync();
    tss->dump_stats();
    delete tss;

    /* Write out a checksums file which lists the checksums for all the
     * segments included in this snapshot.  The format is designed so that it
     * may be easily verified using the sha1sums command. */
    const char csum_type[] = "sha1";
    string checksum_filename = "snapshot-";
    if (backup_scheme.size() > 0)
        checksum_filename += backup_scheme + "-";
    checksum_filename
        = checksum_filename + timestamp + "." + csum_type + "sums";
    RemoteFile *checksum_file = remote->alloc_file(checksum_filename,
                                                   "meta");
    FILE *checksums = fdopen(checksum_file->get_fd(), "w");

    std::set<string> segment_list = db->GetUsedSegments();
    for (std::set<string>::iterator i = segment_list.begin();
         i != segment_list.end(); ++i) {
        map<string, string> segment_metadata = db->GetSegmentMetadata(*i);
        if (segment_metadata.count("path")
            && segment_metadata.count("checksum"))
        {
            string seg_path = segment_metadata["path"];
            string seg_csum = segment_metadata["checksum"];
            const char *raw_checksum = NULL;
            if (strncmp(seg_csum.c_str(), csum_type,
                        strlen(csum_type)) == 0) {
                raw_checksum = seg_csum.c_str() + strlen(csum_type);
                if (*raw_checksum == '=')
                    raw_checksum++;
                else
                    raw_checksum = NULL;
            }

            if (raw_checksum != NULL)
                fprintf(checksums, "%s *%s\n",
                        raw_checksum, seg_path.c_str());
        }
    }
    fclose(checksums);

    SHA1Checksum checksum_csum;
    string csum;
    checksum_filename = checksum_file->get_local_path();
    if (checksum_csum.process_file(checksum_filename.c_str())) {
        csum = checksum_csum.checksum_str();
    }

    checksum_file->send();

    /* Write out a summary file with metadata for all the segments in this
     * snapshot (can be used to reconstruct database contents if needed). */
    string dbmeta_filename = "snapshot-";
    if (backup_scheme.size() > 0)
        dbmeta_filename += backup_scheme + "-";
    dbmeta_filename += timestamp + ".meta" + filter_extension;
    RemoteFile *dbmeta_file = remote->alloc_file(dbmeta_filename, "meta");
    FileFilter *dbmeta_filter = FileFilter::New(dbmeta_file->get_fd(),
                                                filter_program);
    if (dbmeta_filter == NULL) {
        fprintf(stderr, "Unable to open descriptor output file: %m\n");
        return 1;
    }
    FILE *dbmeta = fdopen(dbmeta_filter->get_wrapped_fd(), "w");

    for (std::set<string>::iterator i = segment_list.begin();
         i != segment_list.end(); ++i) {
        map<string, string> segment_metadata = db->GetSegmentMetadata(*i);
        if (segment_metadata.size() > 0) {
            map<string, string>::const_iterator j;
            for (j = segment_metadata.begin();
                 j != segment_metadata.end(); ++j)
            {
                fprintf(dbmeta, "%s: %s\n",
                        j->first.c_str(), j->second.c_str());
            }
            fprintf(dbmeta, "\n");
        }
    }
    fclose(dbmeta);

    string dbmeta_csum
        = Hash::hash_file(dbmeta_file->get_local_path().c_str());
    dbmeta_file->send();

    db->Close();

    /* All other files should be flushed to remote storage before writing the
     * backup descriptor below, so that it is not possible to have a backup
     * descriptor written out depending on non-existent (not yet written)
     * files. */
    remote->sync();

    /* Write a backup descriptor file, which says which segments are needed and
     * where to start to restore this snapshot.  The filename is based on the
     * current time.  If a signature filter program was specified, filter the
     * data through that to give a chance to sign the descriptor contents. */
    string desc_filename = "snapshot-";
    if (backup_scheme.size() > 0)
        desc_filename += backup_scheme + "-";
    desc_filename = desc_filename + timestamp + ".cumulus";

    RemoteFile *descriptor_file = remote->alloc_file(desc_filename,
                                                     "snapshots");
    FileFilter *descriptor_filter = FileFilter::New(descriptor_file->get_fd(),
                                                    signature_filter.c_str());
    if (descriptor_filter == NULL) {
        fprintf(stderr, "Unable to open descriptor output file: %m\n");
        return 1;
    }
    FILE *descriptor = fdopen(descriptor_filter->get_wrapped_fd(), "w");

    fprintf(descriptor, "Format: Cumulus Snapshot v0.11\n");
    fprintf(descriptor, "Producer: Cumulus %s\n", cumulus_version);
    string timestamp_local
        = TimeFormat::format(now, TimeFormat::FORMAT_LOCALTIME, false);
    fprintf(descriptor, "Date: %s\n", timestamp_local.c_str());
    if (backup_scheme.size() > 0)
        fprintf(descriptor, "Scheme: %s\n", backup_scheme.c_str());
    fprintf(descriptor, "Root: %s\n", backup_root.c_str());

    if (dbmeta_csum.size() > 0) {
        fprintf(descriptor, "Database-state: %s\n", dbmeta_csum.c_str());
    }

    if (csum.size() > 0) {
        fprintf(descriptor, "Checksums: %s\n", csum.c_str());
    }

    fprintf(descriptor, "Segments:\n");
    for (std::set<string>::iterator i = segment_list.begin();
         i != segment_list.end(); ++i) {
        fprintf(descriptor, "    %s\n", i->c_str());
    }

    fclose(descriptor);
    if (descriptor_filter->wait() < 0) {
        fatal("Signature filter process error");
    }

    descriptor_file->send();

    remote->sync();
    delete remote;

    if (backup_script != "") {
        if (rmdir(tmp_dir.c_str()) < 0) {
            fprintf(stderr,
                    "Warning: Cannot delete temporary directory %s: %m\n",
                    tmp_dir.c_str());
        }
    }

    return 0;
}
