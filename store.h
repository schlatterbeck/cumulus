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

/* Backup data is stored in a collection of objects, which are grouped together
 * into segments for storage purposes.  This implementation of the object store
 * represents segments as TAR files and objects as files within them. */

#ifndef _LBS_STORE_H
#define _LBS_STORE_H

#include <stdint.h>

#include <list>
#include <map>
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

    /* Filter support. */
    int real_fd, filter_fd;
    pid_t filter_pid;

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
                                 const std::string &group = "");

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
        int size;                   // Combined size of objects written
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
    // called.
    void set_data(const char *d, size_t len) { data = d; data_len = len; }

    // Write an object to a segment, thus making it permanent.  This function
    // can be called at most once.
    void write(TarSegmentStore *store);

    // Compute the checksum of an object, and include it in the object
    // reference.  This should be called after write(), and the data specified
    // by set_data() must remain valid through the call to checksum().
    void checksum();

    // An object is assigned a permanent name once it has been written to a
    // segment.  Until that time, its name cannot be determined.
    std::string get_name() const { return ref.to_string(); }
    ObjectReference get_ref() { return ref; }

private:
    std::string group;
    const char *data;
    size_t data_len;

    bool written;
    ObjectReference ref;
};

/* Program through which segment data is piped before being written to file. */
extern const char *filter_program;

/* Extension which should be appended to segments written out (.tar is already
 * included; this adds to it) */
extern const char *filter_extension;

/* Launch a process to filter data written to a file descriptor.  fd_out is the
 * file descriptor where the filtered data should be written.  program is the
 * filter program to execute (a single string which will be interpreted by
 * /bin/sh).  The return value is a file descriptor to which the data to be
 * filtered should be written.  The process ID of the filter process is stored
 * at address filter_pid if non-NULL. */
int spawn_filter(int fd_out, const char *program, pid_t *filter_pid);

#endif // _LBS_STORE_H
