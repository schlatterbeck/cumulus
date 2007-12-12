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
#include "store.h"
#include "sha1.h"
#include "util.h"

using std::list;
using std::string;
using std::vector;
using std::ostream;

/* Version information.  This will be filled in by the Makefile. */
#ifndef LBS_VERSION
#define LBS_VERSION Unknown
#endif
#define LBS_STRINGIFY(s) LBS_STRINGIFY2(s)
#define LBS_STRINGIFY2(s) #s
static const char lbs_version[] = LBS_STRINGIFY(LBS_VERSION);

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
     * re-reading the entire contents. */
    bool cached = false;

    if (metawriter->find(path) && metawriter->is_unchanged(&stat_buf)) {
        cached = true;
        list<ObjectReference> blocks = metawriter->get_blocks();

        /* If any of the blocks in the object have been expired, then we should
         * fall back to fully reading in the file. */
        for (list<ObjectReference>::const_iterator i = blocks.begin();
             i != blocks.end(); ++i) {
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
            for (list<ObjectReference>::const_iterator i = blocks.begin();
                 i != blocks.end(); ++i) {
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
                ObjectReference ref = db->FindObject(block_csum, bytes);
            }

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

                o->set_data(block_buf, bytes);
                o->write(tss);
                ref = o->get_ref();
                db->StoreObject(ref, block_csum, bytes, block_age);
                delete o;
            }

            object_list.push_back(ref.to_string());
            if (ref.is_normal())
                add_segment(ref.get_segment());
            db->UseObject(ref);
            size += bytes;

            if (status == NULL)
                status = "old";
        }

        file_info["checksum"] = hash.checksum_str();
    }

    if (status != NULL)
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
        "LBS %s\n\n"
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
        "                           (defaults to \".bz2\")\n"
        "  --signature-filter=COMMAND\n"
        "                       program though which to filter descriptor\n"
        "  --scheme=NAME        optional name for this snapshot\n"
        "  --intent=FLOAT       intended backup type: 1=daily, 7=weekly, ...\n"
        "                           (defaults to \"1\")\n",
        lbs_version, program
    );
}

int main(int argc, char *argv[])
{
    string backup_dest = "";
    string localdb_dir = "";
    string backup_scheme = "";
    string signature_filter = "";

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
            default:
                fprintf(stderr, "Unhandled long option!\n");
                return 1;
            }
        } else {
            usage(argv[0]);
            return 1;
        }
    }

    if (optind == argc) {
        usage(argv[0]);
        return 1;
    }

    searches.push_back(".");
    for (int i = optind; i < argc; i++)
        add_include(argv[i]);

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

        printf("LBS Version: %s\n", lbs_version);

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
    db->Open(database_path.c_str(), desc_buf,
             backup_scheme.size() ? backup_scheme.c_str() : NULL,
             snapshot_intent);

    tss = new TarSegmentStore(backup_dest, db);

    /* Initialize the stat cache, for skipping over unchanged files. */
    metawriter = new MetadataWriter(tss, localdb_dir.c_str(), desc_buf,
                                    backup_scheme.size()
                                        ? backup_scheme.c_str()
                                        : NULL);

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
    string checksum_filename = backup_dest + "/snapshot-";
    if (backup_scheme.size() > 0)
        checksum_filename += backup_scheme + "-";
    checksum_filename = checksum_filename + desc_buf + "." + csum_type + "sums";
    FILE *checksums = fopen(checksum_filename.c_str(), "w");
    if (checksums != NULL) {
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
    } else {
        fprintf(stderr, "ERROR: Unable to write checksums file: %m\n");
    }

    db->Close();

    /* Write a backup descriptor file, which says which segments are needed and
     * where to start to restore this snapshot.  The filename is based on the
     * current time.  If a signature filter program was specified, filter the
     * data through that to give a chance to sign the descriptor contents. */
    string desc_filename = backup_dest + "/snapshot-";
    if (backup_scheme.size() > 0)
        desc_filename += backup_scheme + "-";
    desc_filename = desc_filename + desc_buf + ".lbs";

    int descriptor_fd = open(desc_filename.c_str(), O_WRONLY | O_CREAT, 0666);
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

    fprintf(descriptor, "Format: LBS Snapshot v0.6\n");
    fprintf(descriptor, "Producer: LBS %s\n", lbs_version);
    strftime(desc_buf, sizeof(desc_buf), "%Y-%m-%d %H:%M:%S %z", &time_buf);
    fprintf(descriptor, "Date: %s\n", desc_buf);
    if (backup_scheme.size() > 0)
        fprintf(descriptor, "Scheme: %s\n", backup_scheme.c_str());
    fprintf(descriptor, "Root: %s\n", backup_root.c_str());

    SHA1Checksum checksum_csum;
    if (checksum_csum.process_file(checksum_filename.c_str())) {
        string csum = checksum_csum.checksum_str();
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

    return 0;
}
