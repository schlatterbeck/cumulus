/* LBS: An LFS-inspired filesystem backup system
 * Copyright (C) 2006  Michael Vrable
 *
 * Backup data is stored in a collection of objects, which are grouped together
 * into segments for storage purposes.  This implementation of the object store
 * is built on top of libtar, and represents segments as TAR files and objects
 * as files within them. */

#ifndef _LBS_TARSTORE_H
#define _LBS_TARSTORE_H

#include <stdint.h>
#include <libtar.h>

#include <list>
#include <map>
#include <set>
#include <string>
#include <iostream>
#include <sstream>

#include "sha1.h"

class LbsObject;

/* In memory datatype to represent key/value pairs of information, such as file
 * metadata.  Currently implemented as map<string, string>. */
typedef std::map<std::string, std::string> dictionary;

/* IOException will be thrown if an error occurs while reading or writing in
 * one of the I/O wrappers.  Depending upon the context; this may be fatal or
 * not--typically, errors reading/writing the store will be serious, but errors
 * reading an individual file are less so. */
class IOException : public std::exception {
private:
    std::string error;
public:
    explicit IOException(const std::string &err) { error = err; }
    virtual ~IOException() throw () { }
    std::string getError() const { return error; }
};

/* A simple wrapper around a single TAR file to represent a segment.  Objects
 * may only be written out all at once, since the tar header must be written
 * first; incremental writing is not supported. */
class Tarfile {
public:
    Tarfile(const std::string &path, const std::string &segment);
    ~Tarfile();

    void write_object(int id, const char *data, size_t len);

    // Return an estimate of the size of the file.
    size_t size_estimate() { return size; }

    void internal_write_object(const std::string &path,
                               const char *data, size_t len);

private:
    size_t size;
    std::string segment_name;
    std::ostringstream checksums;
    TAR *t;
};

class TarSegmentStore {
public:
    // New segments will be stored in the given directory.
    TarSegmentStore(const std::string &path) { this->path = path; }
    ~TarSegmentStore() { sync(); }

    // Writes an object to segment in the store, and returns the name
    // (segment/object) to refer to it.  The optional parameter group can be
    // used to control object placement; objects with different group
    // parameters are kept in separate segments.
    std::string write_object(const char *data, size_t len,
                             const std::string &group = "",
                             const std::list<std::string> &refs = norefs);

    // Ensure all segments have been fully written.
    void sync();

private:
    struct segment_info {
        Tarfile *file;
        std::string name;           // UUID
        std::set<std::string> refs; // Other segments this one refers to
        int count;                  // Objects written to this segment
    };

    std::string path;
    std::map<std::string, struct segment_info *> segments;

    // An empty list which can be used as an argument to write_object to
    // indicate that this object depends on no others.
    static std::list<std::string> norefs;

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
    //const char *get_data() const { return data; }
    //size_t get_data_len() const { return data_len; }
    void set_data(const char *d, size_t len) { data = d; data_len = len; }

    // Write an object to a segment, thus making it permanent.  This function
    // can be called at most once.
    void write(TarSegmentStore *store);

    // An object is assigned a permanent name once it has been written to a
    // segment.  Until that time, its name cannot be determined.
    std::string get_name() const { return name; }

    // Logically, one object may reference other objects (such as a metadata
    // listing referncing actual file data blocks).  Such references should be
    // noted explicitly.  It may eventually be used to build up a tree of
    // checksums for later verifying integrity.
    void add_reference(const LbsObject *o);

private:
    std::string group;
    const char *data;
    size_t data_len;

    bool written;
    std::string name;

    std::set<std::string> refs;
};

#endif // _LBS_TARSTORE_H
