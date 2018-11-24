/* Cumulus: Efficient Filesystem Backup to the Cloud
 * Copyright (C) 2006-2008 The Cumulus Developers
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

/* Backup data is stored in a collection of objects, which are grouped together
 * into segments for storage purposes.  This implementation of the object store
 * represents segments as TAR files and objects as files within them. */

#ifndef _LBS_STORE_H
#define _LBS_STORE_H

#include <stdint.h>

#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <iostream>
#include <sstream>

#include "localdb.h"
#include "remote.h"
#include "ref.h"
#include "third_party/sha1.h"

class LbsObject;

/* In memory datatype to represent key/value pairs of information, such as file
 * metadata.  Currently implemented as map<string, string>. */
typedef std::map<std::string, std::string> dictionary;

/* Simplified TAR header--we only need to store regular files, don't need to
 * handle long filenames, etc. */
static const int TAR_BLOCK_SIZE = 512;

struct tar_header
{
    char name[100];
    char mode[8];
    char uid[8];
    char gid[8];
    char size[12];
    char mtime[12];
    char chksum[8];
    char typeflag;
    char linkname[100];
    char magic[8];
    char uname[32];
    char gname[32];
    char devmajor[8];
    char devminor[8];
    char prefix[155];
    char padding[12];
};

class FileFilter {
public:
    // It is valid for program to be NULL or empty; if so, no filtering is
    // done.
    static FileFilter *New(int fd, const char *program);

    // Wait for the filter process to terminate.
    int wait();

    // Accessors for the file descriptors.
    int get_raw_fd() const { return fd_raw; }
    int get_wrapped_fd() const { return fd_wrapped; }

private:
    FileFilter(int raw, int wrapped, pid_t pid);

    // Launch a process to filter data written to a file descriptor.  fd_out is
    // the file descriptor where the filtered data should be written.  program
    // is the filter program to execute (a single string which will be
    // interpreted by /bin/sh).  The return value is a file descriptor to which
    // the data to be filtered should be written.  The process ID of the filter
    // process is stored at address filter_pid if non-NULL.
    static int spawn_filter(int fd_out, const char *program, pid_t *filter_pid);

    // The original file descriptor passed when creating the FileFilter object.
    int fd_raw;

    // The wrapped file descriptor: writes here are piped through the filter
    // program.
    int fd_wrapped;

    // The filter process if one was launched, or -1 if there is no filter
    // program.
    pid_t pid;
};

/* A simple wrapper around a single TAR file to represent a segment.  Objects
 * may only be written out all at once, since the tar header must be written
 * first; incremental writing is not supported. */
class Tarfile {
public:
    Tarfile(RemoteFile *file, const std::string &segment);
    ~Tarfile();

    void write_object(int id, const char *data, size_t len);

    // Return an estimate of the size of the file.
    size_t size_estimate();

private:
    size_t size;
    std::string segment_name;

    RemoteFile *file;
    std::unique_ptr<FileFilter> filter;

    // Write data to the tar file
    void tar_write(const char *data, size_t size);
};

class TarSegmentStore {
public:
    // New segments will be stored in the given directory.
    TarSegmentStore(RemoteStore *remote,
                    LocalDb *db = NULL)
        { this->remote = remote; this->db = db; }
    ~TarSegmentStore() { sync(); }

    // Writes an object to segment in the store, and returns the name
    // (segment/object) to refer to it.  The optional parameter group can be
    // used to control object placement; objects with different group
    // parameters are kept in separate segments.
    ObjectReference write_object(const char *data, size_t len,
                                 const std::string &group = "",
                                 const std::string &checksum = "",
                                 double age = 0.0);

    // Ensure all segments have been fully written.
    void sync();

    // Dump statistics to stdout about how much data has been written
    void dump_stats();

private:
    struct segment_info {
        Tarfile *file;
        std::string group;
        std::string name;           // UUID
        int count;                  // Objects written to this segment
        int data_size;              // Combined size of objects written
        std::string basename;       // Name of segment without directory
        RemoteFile *rf;
    };

    RemoteStore *remote;
    std::map<std::string, struct segment_info *> segments;
    LocalDb *db;

    // Ensure that all segments in the given group have been fully written.
    void close_segment(const std::string &group);

    // Parse an object reference string and return just the segment name
    // portion.
    std::string object_reference_to_segment(const std::string &object);
};

/* An in-memory representation of an object, which can be incrementally built
 * before it is written out to a segment. */
class LbsObject {
public:
    LbsObject();
    ~LbsObject();

    // If an object is placed in a group, it will be written out to segments
    // only containing other objects in the same group.  A group name is simply
    // a string.
    //std::string get_group() const { return group; }
    void set_group(const std::string &g) { group = g; }

    // Data in an object must be written all at once, and cannot be generated
    // incrementally.  Data can be an arbitrary block of binary data of any
    // size.  The pointer to the data need only remain valid until write() is
    // called.  If checksum is non-NULL then it is assumed to contain a hash
    // value for the data; this provides an optimization in case the caller has
    // already checksummed the data.  Otherwise the set_data will compute a
    // hash of the data itself.
    void set_data(const char *d, size_t len, const char *checksum);

    // Explicitly sets the age of the data, for later garbage-collection or
    // repacking purposes.  If not set, the age defaults to the current time.
    // The age is stored in the database as a floating point value, expressing
    // the time in Julian days.
    void set_age(double age) { this->age = age; }

    // Write an object to a segment, thus making it permanent.  This function
    // can be called at most once.
    void write(TarSegmentStore *store);

    // An object is assigned a permanent name once it has been written to a
    // segment.  Until that time, its name cannot be determined.
    ObjectReference get_ref() { return ref; }

private:
    std::string group;
    double age;
    const char *data;
    size_t data_len;
    std::string checksum;

    bool written;
    ObjectReference ref;
};

/* Program through which segment data is piped before being written to file. */
extern const char *filter_program;

/* Extension which should be appended to segments written out (.tar is already
 * included; this adds to it) */
extern const char *filter_extension;

#endif // _LBS_STORE_H
