/* Cumulus: Smart Filesystem Backup to Dumb Servers
 *
 * Copyright (C) 2006-2008  The Regents of the University of California
 * Written by Michael Vrable <mvrable@cs.ucsd.edu>
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

/* Main entry point for LBS.  Contains logic for traversing the filesystem and
 * constructing a backup. */

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
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "localdb.h"
#include "metadata.h"
#include "remote.h"
#include "store.h"
#include "sha1.h"
#include "subfile.h"
#include "util.h"

using std::list;
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

/* Keep track of all segments which are needed to reconstruct the snapshot. */
std::set<string> segment_list;

/* Snapshot intent: 1=daily, 7=weekly, etc.  This is not used directly, but is
 * stored in the local database and can help guide segment cleaning and
 * snapshot expiration policies. */
double snapshot_intent = 1.0;

/* Selection of files to include/exclude in the snapshot. */
std::list<string> includes;         // Paths in which files should be saved
std::list<string> excludes;         // Paths which will not be saved
std::list<string> searches;         // Directories we don't want to save, but
                                    //   do want to descend searching for data
                                    //   in included paths

bool relative_paths = true;

bool flag_rebuild_statcache = false;

/* Whether verbose output is enabled. */
bool verbose = false;

/* Ensure that the given segment is listed as a dependency of the current
 * snapshot. */
void add_segment(const string& segment)
{
    segment_list.insert(segment);
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
                if (ref.is_normal())
                    add_segment(ref.get_segment());
                db->UseObject(ref);
            }
            size = stat_buf.st_size;
        }
    }

    /* If the file is new or changed, we must read in the contents a block at a
     * time. */
    if (!cached) {
        SHA1Checksum hash;
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

            hash.process(block_buf, bytes);

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

            SHA1Checksum block_hash;
            block_hash.process(block_buf, bytes);
            string block_csum = block_hash.checksum_str();

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
                        char group[32];
                        sprintf(group, "compacted-%d", object_group);
                        o->set_group(group);
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
                refs.push_back(ref);
            }

            while (!refs.empty()) {
                ref = refs.front(); refs.pop_front();
                object_list.push_back(ref.to_string());
                if (ref.is_normal())
                    add_segment(ref.get_segment());
                db->UseObject(ref);
            }
            size += bytes;

            if (status == NULL)
                status = "old";
        }

        file_info["checksum"] = hash.checksum_str();
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
    file_info["user"] = encode_int(stat_buf.st_uid);
    file_info["group"] = encode_int(stat_buf.st_gid);

    time_t now = time(NULL);
    if (now - stat_buf.st_ctime < 30 || now - stat_buf.st_mtime < 30)
        if ((stat_buf.st_mode & S_IFMT) != S_IFDIR)
            file_info["volatile"] = "1";

    struct passwd *pwd = getpwuid(stat_buf.st_uid);
    if (pwd != NULL) {
        file_info["user"] += " (" + uri_encode(pwd->pw_name) + ")";
    }

    struct group *grp = getgrgid(stat_buf.st_gid);
    if (pwd != NULL) {
        file_info["group"] += " (" + uri_encode(grp->gr_name) + ")";
    }

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

void scanfile(const string& path, bool include)
{
    int fd = -1;
    long flags;
    struct stat stat_buf;
    list<string> refs;

    string true_path;
    if (relative_paths)
        true_path = path;
    else
        true_path = "/" + path;

    // Set to true if we should scan through the contents of this directory,
    // but not actually back files up
    bool scan_only = false;

    // Check this file against the include/exclude list to see if it should be
    // considered
    for (list<string>::iterator i = includes.begin();
         i != includes.end(); ++i) {
        if (path == *i) {
            include = true;
        }
    }

    for (list<string>::iterator i = excludes.begin();
         i != excludes.end(); ++i) {
        if (path == *i) {
            include = false;
        }
    }

    for (list<string>::iterator i = searches.begin();
         i != searches.end(); ++i) {
        if (path == *i) {
            scan_only = true;
        }
    }

    if (!include && !scan_only)
        return;

    if (lstat(true_path.c_str(), &stat_buf) < 0) {
        fprintf(stderr, "lstat(%s): %m\n", path.c_str());
        return;
    }

    if ((stat_buf.st_mode & S_IFMT) == S_IFREG) {
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
        fd = open(true_path.c_str(), O_RDONLY|O_NOATIME|O_NOFOLLOW|O_NONBLOCK);
        if (fd < 0) {
            fd = open(true_path.c_str(), O_RDONLY|O_NOFOLLOW|O_NONBLOCK);
        }
        if (fd < 0) {
            fprintf(stderr, "Unable to open file %s: %m\n", path.c_str());
            return;
        }

        /* Drop the use of the O_NONBLOCK flag; we only wanted that for file
         * open. */
        flags = fcntl(fd, F_GETFL);
        fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);

        /* Perform the stat call again, and check that we still have a regular
         * file. */
        if (fstat(fd, &stat_buf) < 0) {
            fprintf(stderr, "fstat: %m\n");
            close(fd);
            return;
        }

        if ((stat_buf.st_mode & S_IFMT) != S_IFREG) {
            fprintf(stderr, "file is no longer a regular file!\n");
            close(fd);
            return;
        }
    }

    dump_inode(path, true_path, stat_buf, fd);

    if (fd >= 0)
        close(fd);

    // If we hit a directory, now that we've written the directory itself,
    // recursively scan the directory.
    if ((stat_buf.st_mode & S_IFMT) == S_IFDIR) {
        DIR *dir = opendir(true_path.c_str());

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

        closedir(dir);

        sort(contents.begin(), contents.end());

        for (vector<string>::iterator i = contents.begin();
             i != contents.end(); ++i) {
            const string& filename = *i;
            if (path == ".")
                scanfile(filename, include);
            else
                scanfile(path + "/" + filename, include);
        }
    }
}

/* Include the specified file path in the backups.  Append the path to the
 * includes list, and to ensure that we actually see the path when scanning the
 * directory tree, add all the parent directories to the search list, which
 * means we will scan through the directory listing even if the files
 * themselves are excluded from being backed up. */
void add_include(const char *path)
{
    /* Was an absolute path specified?  If so, we'll need to start scanning
     * from the root directory.  Make sure that the user was consistent in
     * providing either all relative paths or all absolute paths. */
    if (path[0] == '/') {
        if (includes.size() > 0 && relative_paths == true) {
            fprintf(stderr,
                    "Error: Cannot mix relative and absolute paths!\n");
            exit(1);
        }

        relative_paths = false;

        // Skip over leading '/'
        path++;
    } else if (relative_paths == false && path[0] != '/') {
        fprintf(stderr, "Error: Cannot mix relative and absolute paths!\n");
        exit(1);
    }

    includes.push_back(path);

    /* Split the specified path into directory components, and ensure that we
     * descend into all the directories along the path. */
    const char *slash = path;

    if (path[0] == '\0')
        return;

    while ((slash = strchr(slash + 1, '/')) != NULL) {
        string component(path, slash - path);
        searches.push_back(component);
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
        "  --exclude=PATH       exclude files in PATH from snapshot\n"
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
        "  --intent=FLOAT       intended backup type: 1=daily, 7=weekly, ...\n"
        "                           (defaults to \"1\")\n"
        "  --full-metadata      do not re-use metadata from previous backups\n"
        "  -v --verbose         list files as they are backed up\n"
        "\n"
        "Exactly one of --dest or --upload-script must be specified.\n",
        cumulus_version, program
    );
}

int main(int argc, char *argv[])
{
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
            {"exclude", 1, 0, 0},           // 1
            {"filter", 1, 0, 0},            // 2
            {"filter-extension", 1, 0, 0},  // 3
            {"dest", 1, 0, 0},              // 4
            {"scheme", 1, 0, 0},            // 5
            {"signature-filter", 1, 0, 0},  // 6
            {"intent", 1, 0, 0},            // 7
            {"full-metadata", 0, 0, 0},     // 8
            {"tmpdir", 1, 0, 0},            // 9
            {"upload-script", 1, 0, 0},     // 10
            {"rebuild-statcache", 0, 0, 0}, // 11
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
            case 1:     // --exclude
                if (optarg[0] != '/')
                    excludes.push_back(optarg);
                else
                    excludes.push_back(optarg + 1);
                break;
            case 2:     // --filter
                filter_program = optarg;
                break;
            case 3:     // --filter-extension
                filter_extension = optarg;
                break;
            case 4:     // --dest
                backup_dest = optarg;
                break;
            case 5:     // --scheme
                backup_scheme = optarg;
                break;
            case 6:     // --signature-filter
                signature_filter = optarg;
                break;
            case 7:     // --intent
                snapshot_intent = atof(optarg);
                if (snapshot_intent <= 0)
                    snapshot_intent = 1;
                break;
            case 8:     // --full-metadata
                flag_full_metadata = true;
                break;
            case 9:     // --tmpdir
                tmp_dir = optarg;
                break;
            case 10:    // --upload-script
                backup_script = optarg;
                break;
            case 11:    // --rebuild-statcache
                flag_rebuild_statcache = true;
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

    searches.push_back(".");
    for (int i = optind; i < argc; i++)
        add_include(argv[i]);

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
        tmp_dir = tmp_dir + "/lbs." + generate_uuid();
        if (mkdir(tmp_dir.c_str(), 0700) < 0) {
            fprintf(stderr, "Cannot create temporary directory %s: %m\n",
                    tmp_dir.c_str());
            return 1;
        }
        remote = new RemoteStore(tmp_dir);
        remote->set_script(backup_script);
    } else {
        remote = new RemoteStore(backup_dest);
    }

    /* Store the time when the backup started, so it can be included in the
     * snapshot name. */
    time_t now;
    struct tm time_buf;
    char desc_buf[256];
    time(&now);
    localtime_r(&now, &time_buf);
    strftime(desc_buf, sizeof(desc_buf), "%Y%m%dT%H%M%S", &time_buf);

    /* Open the local database which tracks all objects that are stored
     * remotely, for efficient incrementals.  Provide it with the name of this
     * snapshot. */
    string database_path = localdb_dir + "/localdb.sqlite";
    db = new LocalDb;
    db->Open(database_path.c_str(), desc_buf, backup_scheme.c_str(),
             snapshot_intent);

    tss = new TarSegmentStore(remote, db);

    /* Initialize the stat cache, for skipping over unchanged files. */
    metawriter = new MetadataWriter(tss, localdb_dir.c_str(), desc_buf,
                                    backup_scheme.c_str());

    scanfile(".", false);

    ObjectReference root_ref = metawriter->close();
    add_segment(root_ref.get_segment());
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
    checksum_filename = checksum_filename + desc_buf + "." + csum_type + "sums";
    RemoteFile *checksum_file = remote->alloc_file(checksum_filename,
                                                   "checksums");
    FILE *checksums = fdopen(checksum_file->get_fd(), "w");

    for (std::set<string>::iterator i = segment_list.begin();
         i != segment_list.end(); ++i) {
        string seg_path, seg_csum;
        if (db->GetSegmentChecksum(*i, &seg_path, &seg_csum)) {
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
    desc_filename = desc_filename + desc_buf + ".lbs";

    RemoteFile *descriptor_file = remote->alloc_file(desc_filename,
                                                     "snapshots");
    int descriptor_fd = descriptor_file->get_fd();
    if (descriptor_fd < 0) {
        fprintf(stderr, "Unable to open descriptor output file: %m\n");
        return 1;
    }
    pid_t signature_pid = 0;
    if (signature_filter.size() > 0) {
        int new_fd = spawn_filter(descriptor_fd, signature_filter.c_str(),
                                  &signature_pid);
        close(descriptor_fd);
        descriptor_fd = new_fd;
    }
    FILE *descriptor = fdopen(descriptor_fd, "w");

    fprintf(descriptor, "Format: LBS Snapshot v0.8\n");
    fprintf(descriptor, "Producer: Cumulus %s\n", cumulus_version);
    strftime(desc_buf, sizeof(desc_buf), "%Y-%m-%d %H:%M:%S %z", &time_buf);
    fprintf(descriptor, "Date: %s\n", desc_buf);
    if (backup_scheme.size() > 0)
        fprintf(descriptor, "Scheme: %s\n", backup_scheme.c_str());
    fprintf(descriptor, "Backup-Intent: %g\n", snapshot_intent);
    fprintf(descriptor, "Root: %s\n", backup_root.c_str());

    if (csum.size() > 0) {
        fprintf(descriptor, "Checksums: %s\n", csum.c_str());
    }

    fprintf(descriptor, "Segments:\n");
    for (std::set<string>::iterator i = segment_list.begin();
         i != segment_list.end(); ++i) {
        fprintf(descriptor, "    %s\n", i->c_str());
    }

    fclose(descriptor);

    if (signature_pid) {
        int status;
        waitpid(signature_pid, &status, 0);

        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            throw IOException("Signature filter process error");
        }
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
