/* Recursively descend the filesystem and visit each file. */

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <grp.h>
#include <pwd.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <list>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "format.h"
#include "localdb.h"
#include "store.h"
#include "sha1.h"
#include "statcache.h"

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

/* Stat cache, which stored data locally to speed the backup process by quickly
 * skipping files which have not changed. */
StatCache *statcache;

/* Contents of the root object.  This will contain a set of indirect links to
 * the metadata objects. */
std::ostringstream metadata_root;

/* Buffer for building up metadata. */
std::ostringstream metadata;

/* Keep track of all segments which are needed to reconstruct the snapshot. */
std::set<string> segment_list;

void scandir(const string& path, bool include);

/* Selection of files to include/exclude in the snapshot. */
std::list<string> includes;         // Paths in which files should be saved
std::list<string> excludes;         // Paths which will not be saved
std::list<string> searches;         // Directories we don't want to save, but
                                    //   do want to descend searching for data
                                    //   in included paths

bool relative_paths = true;

/* Ensure contents of metadata are flushed to an object. */
void metadata_flush()
{
    string m = metadata.str();
    if (m.size() == 0)
        return;

    /* Write current metadata information to a new object. */
    LbsObject *meta = new LbsObject;
    meta->set_group("metadata");
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
int64_t dumpfile(int fd, dictionary &file_info, const string &path)
{
    struct stat stat_buf;
    fstat(fd, &stat_buf);
    int64_t size = 0;
    list<string> object_list;

    if ((stat_buf.st_mode & S_IFMT) != S_IFREG) {
        fprintf(stderr, "file is no longer a regular file!\n");
        return -1;
    }

    /* Look up this file in the old stat cache, if we can.  If the stat
     * information indicates that the file has not changed, do not bother
     * re-reading the entire contents. */
    bool cached = false;

    if (statcache->Find(path, &stat_buf)) {
        cached = true;
        const list<ObjectReference> &blocks = statcache->get_blocks();

        /* If any of the blocks in the object have been expired, then we should
         * fall back to fully reading in the file. */
        for (list<ObjectReference>::const_iterator i = blocks.begin();
             i != blocks.end(); ++i) {
            const ObjectReference &ref = *i;
            if (!db->IsAvailable(ref)) {
                cached = false;
                break;
            }
        }

        /* If everything looks okay, use the cached information */
        if (cached) {
            file_info["checksum"] = statcache->get_checksum();
            for (list<ObjectReference>::const_iterator i = blocks.begin();
                 i != blocks.end(); ++i) {
                const ObjectReference &ref = *i;
                object_list.push_back(ref.to_string());
                segment_list.insert(ref.get_segment());
                db->UseObject(ref);
            }
            size = stat_buf.st_size;
        }
    }

    /* If the file is new or changed, we must read in the contents a block at a
     * time. */
    if (!cached) {
        printf("    [new]\n");

        SHA1Checksum hash;
        while (true) {
            size_t bytes = file_read(fd, block_buf, LBS_BLOCK_SIZE);
            if (bytes == 0)
                break;

            hash.process(block_buf, bytes);

            // Either find a copy of this block in an already-existing segment,
            // or index it so it can be re-used in the future
            double block_age = 0.0;
            SHA1Checksum block_hash;
            block_hash.process(block_buf, bytes);
            string block_csum = block_hash.checksum_str();
            ObjectReference ref = db->FindObject(block_csum, bytes);

            // Store a copy of the object if one does not yet exist
            if (ref.get_segment().size() == 0) {
                LbsObject *o = new LbsObject;

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
                if (db->IsOldObject(block_csum, bytes, &block_age))
                    o->set_group("compacted");
                else
                    o->set_group("data");

                o->set_data(block_buf, bytes);
                o->write(tss);
                ref = o->get_ref();
                db->StoreObject(ref, block_csum, bytes, block_age);
                delete o;
            }

            object_list.push_back(ref.to_string());
            segment_list.insert(ref.get_segment());
            db->UseObject(ref);
            size += bytes;
        }

        file_info["checksum"] = hash.checksum_str();
    }

    statcache->Save(path, &stat_buf, file_info["checksum"], object_list);

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
        i->set_group("metadata");
        i->set_data(blocklist.data(), blocklist.size());
        i->write(tss);
        file_info["data"] = "@" + i->get_name();
        segment_list.insert(i->get_ref().get_segment());
        delete i;
    }

    return size;
}

void scanfile(const string& path, bool include)
{
    int fd;
    long flags;
    struct stat stat_buf;
    char *buf;
    ssize_t len;
    int64_t file_size;
    list<string> refs;

    string true_path;
    if (relative_paths)
        true_path = path;
    else
        true_path = "/" + path;

    // Set to true if the item is a directory and we should recursively scan
    bool recurse = false;

    // Set to true if we should scan through the contents of this directory,
    // but not actually back files up
    bool scan_only = false;

    // Check this file against the include/exclude list to see if it should be
    // considered
    for (list<string>::iterator i = includes.begin();
         i != includes.end(); ++i) {
        if (path == *i) {
            printf("Including %s\n", path.c_str());
            include = true;
        }
    }

    for (list<string>::iterator i = excludes.begin();
         i != excludes.end(); ++i) {
        if (path == *i) {
            printf("Excluding %s\n", path.c_str());
            include = false;
        }
    }

    for (list<string>::iterator i = searches.begin();
         i != searches.end(); ++i) {
        if (path == *i) {
            printf("Scanning %s\n", path.c_str());
            scan_only = true;
        }
    }

    if (!include && !scan_only)
        return;

    dictionary file_info;

    lstat(true_path.c_str(), &stat_buf);

    printf("%s\n", path.c_str());

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
        len = readlink(true_path.c_str(), buf, stat_buf.st_size + 1);
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

        file_size = dumpfile(fd, file_info, path);
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

    metadata << "name: " << uri_encode(path) << "\n";
    dict_output(metadata, file_info);
    metadata << "\n";

    // Break apart metadata listing if it becomes too large.
    if (metadata.str().size() > LBS_METADATA_BLOCK_SIZE)
        metadata_flush();

    // If we hit a directory, now that we've written the directory itself,
    // recursively scan the directory.
    if (recurse)
        scandir(path, include);
}

void scandir(const string& path, bool include)
{
    string true_path;
    if (relative_paths)
        true_path = path;
    else
        true_path = "/" + path;

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

    sort(contents.begin(), contents.end());

    for (vector<string>::iterator i = contents.begin();
         i != contents.end(); ++i) {
        const string& filename = *i;
        if (path == ".")
            scanfile(filename, include);
        else
            scanfile(path + "/" + filename, include);
    }

    closedir(dir);
}

/* Include the specified file path in the backups.  Append the path to the
 * includes list, and to ensure that we actually see the path when scanning the
 * directory tree, add all the parent directories to the search list, which
 * means we will scan through the directory listing even if the files
 * themselves are excluded from being backed up. */
void add_include(const char *path)
{
    printf("Add: %s\n", path);
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
        "Usage: %s [OPTION]... --dest=DEST PATHS...\n"
        "Produce backup snapshot of files in SOURCE and store to DEST.\n"
        "\n"
        "Options:\n"
        "  --dest=PATH          path where backup is to be written [REQUIRED]\n"
        "  --exclude=PATH       exclude files in PATH from snapshot\n"
        "  --localdb=PATH       local backup metadata is stored in PATH\n"
        "  --filter=COMMAND     program through which to filter segment data\n"
        "                           (defaults to \"bzip2 -c\")\n"
        "  --filter-extension=EXT\n"
        "                       string to append to segment files\n"
        "                           (defaults to \".bz2\")\n",
        program
    );
}

int main(int argc, char *argv[])
{
    string backup_source = ".";
    string backup_dest = "";
    string localdb_dir = "";

    while (1) {
        static struct option long_options[] = {
            {"localdb", 1, 0, 0},           // 0
            {"exclude", 1, 0, 0},           // 1
            {"filter", 1, 0, 0},            // 2
            {"filter-extension", 1, 0, 0},  // 3
            {"dest", 1, 0, 0},              // 4
            {NULL, 0, 0, 0},
        };

        int long_index;
        int c = getopt_long(argc, argv, "", long_options, &long_index);

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
            default:
                fprintf(stderr, "Unhandled long option!\n");
                return 1;
            }
        } else {
            usage(argv[0]);
            return 1;
        }
    }

    if (argc < optind + 2) {
        usage(argv[0]);
        return 1;
    }

    searches.push_back(".");
    if (optind == argc) {
        add_include(".");
    } else {
        for (int i = optind; i < argc; i++)
            add_include(argv[i]);
    }

    backup_source = argv[optind];

    if (backup_dest == "") {
        fprintf(stderr,
                "Error: Backup destination must be specified with --dest=\n");
        usage(argv[0]);
        return 1;
    }

    // Default for --localdb is the same as --dest
    if (localdb_dir == "") {
        localdb_dir = backup_dest;
    }

    // Dump paths for debugging/informational purposes
    {
        list<string>::const_iterator i;

        printf("--dest=%s\n--localdb=%s\n\n",
               backup_dest.c_str(), localdb_dir.c_str());

        printf("Includes:\n");
        for (i = includes.begin(); i != includes.end(); ++i)
            printf("    %s\n", i->c_str());

        printf("Excludes:\n");
        for (i = excludes.begin(); i != excludes.end(); ++i)
            printf("    %s\n", i->c_str());

        printf("Searching:\n");
        for (i = searches.begin(); i != searches.end(); ++i)
            printf("    %s\n", i->c_str());
    }

    tss = new TarSegmentStore(backup_dest);
    block_buf = new char[LBS_BLOCK_SIZE];

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
    db->Open(database_path.c_str(), desc_buf);

    /* Initialize the stat cache, for skipping over unchanged files. */
    statcache = new StatCache;
    statcache->Open(localdb_dir.c_str(), desc_buf);

    scanfile(".", false);

    metadata_flush();
    const string md = metadata_root.str();

    LbsObject *root = new LbsObject;
    root->set_group("metadata");
    root->set_data(md.data(), md.size());
    root->write(tss);
    root->checksum();
    segment_list.insert(root->get_ref().get_segment());

    string backup_root = root->get_ref().to_string();
    delete root;

    db->Close();

    statcache->Close();
    delete statcache;

    tss->sync();
    tss->dump_stats();
    delete tss;

    /* Write a backup descriptor file, which says which segments are needed and
     * where to start to restore this snapshot.  The filename is based on the
     * current time. */
    string desc_filename = backup_dest + "/snapshot-" + desc_buf + ".lbs";
    std::ofstream descriptor(desc_filename.c_str());

    descriptor << "Format: LBS Snapshot v0.1\n";
    strftime(desc_buf, sizeof(desc_buf), "%Y-%m-%d %H:%M:%S %z", &time_buf);
    descriptor << "Date: " << desc_buf << "\n";
    descriptor << "Root: " << backup_root << "\n";

    descriptor << "Segments:\n";
    for (std::set<string>::iterator i = segment_list.begin();
         i != segment_list.end(); ++i) {
        descriptor << "    " << *i << "\n";
    }

    return 0;
}
