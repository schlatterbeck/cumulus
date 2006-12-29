/* LBS: An LFS-inspired filesystem backup system
 * Copyright (C) 2006  Michael Vrable
 *
 * Backup data is stored in a collection of objects, which are grouped together
 * into segments for storage purposes.  This file provides interfaces for
 * reading and writing objects and segments. */

#ifndef _LBS_STORE_H
#define _LBS_STORE_H

#include <stdint.h>

#include <exception>
#include <map>
#include <string>
#include <sstream>
#include <vector>

#include "sha1.h"

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

/* OutputStream is an abstract interface for writing data without seeking.
 * Output could be to a file, to an object within a segment, or even to a
 * memory buffer to help serialize data. */
class OutputStream {
public:
    OutputStream();
    virtual ~OutputStream() { }

    // Write the given data buffer
    void write(const void *data, size_t len);

    // Return the total number of bytes written so far
    int64_t get_pos() const { return bytes_written; }

    // Convenience functions for writing other data types.  Values are always
    // written out in little-endian order.
    void write_u8(uint8_t val);
    void write_u16(uint16_t val);
    void write_u32(uint32_t val);
    void write_u64(uint64_t val);

    void write_s32(int32_t val) { write_u32((uint32_t)val); }
    void write_s64(int64_t val) { write_u64((uint64_t)val); }

    void write_varint(uint64_t val);

    void write_string(const std::string &s);
    void write_dictionary(const dictionary &d);

protected:
    // Function which actually causes a write: must be overridden by
    // implementation.
    virtual void write_internal(const void *data, size_t len) = 0;

private:
    int64_t bytes_written;
};

/* An OutputStream implementation which writes data to memory and returns the
 * result as a string. */
class StringOutputStream : public OutputStream {
public:
    StringOutputStream();
    std::string contents() const { return buf.str(); }

protected:
    virtual void write_internal(const void *data, size_t len);

private:
    std::stringstream buf;
};

/* An OutputStream implementation which writes data via the C stdio layer. */
class FileOutputStream : public OutputStream {
public:
    explicit FileOutputStream(FILE *file);
    virtual ~FileOutputStream();

protected:
    virtual void write_internal(const void *data, size_t len);

private:
    FILE *f;
};

/* An OutputStream which is simply sends writes to another OutputStream, but
 * does provide separate tracking of bytes written. */
class WrapperOutputStream : public OutputStream {
public:
    explicit WrapperOutputStream(OutputStream &o);
    virtual ~WrapperOutputStream() { }

protected:
    virtual void write_internal(const void *data, size_t len);

private:
    OutputStream &real;
};

/* Like WrapperOutputStream, but additionally computes a checksum of data as it
 * is written. */
class ChecksumOutputStream : public OutputStream {
public:
    explicit ChecksumOutputStream(OutputStream &o);
    virtual ~ChecksumOutputStream() { }

    /* Once a checksum is computed, no further data should be written to the
     * stream. */
    const uint8_t *finish_and_checksum();
    size_t checksum_size() const { return csum.checksum_size(); }

protected:
    virtual void write_internal(const void *data, size_t len);

private:
    OutputStream &real;
    SHA1Checksum csum;
};

/* Simple wrappers that encode integers using a StringOutputStream and return
 * the encoded result. */
std::string encode_u16(uint16_t val);
std::string encode_u32(uint32_t val);
std::string encode_u64(uint64_t val);

struct uuid {
    uint8_t bytes[16];
};

/* A class which is used to pack multiple objects into a single segment, with a
 * lookup table to quickly locate each object.  Call new_object() to get an
 * OutputStream to which a new object may be written, and optionally
 * finish_object() when finished writing the current object.  Only one object
 * may be written to a segment at a time; if multiple objects must be written
 * concurrently, they must be to different segments. */
class SegmentWriter {
public:
    SegmentWriter(OutputStream *output, struct uuid u);
    ~SegmentWriter();

    struct uuid get_uuid() const { return id; }

    // Start writing out a new object to this segment.
    OutputStream *new_object();
    void finish_object();

    // Utility functions for generating and formatting UUIDs for display.
    static struct uuid generate_uuid();
    static std::string format_uuid(const struct uuid u);

private:
    typedef std::vector<std::pair<int64_t, int64_t> > object_table;

    ChecksumOutputStream *out;  // Output stream with checksumming enabled
    OutputStream *raw_out;      // Raw output stream, without checksumming
    struct uuid id;

    int64_t object_start_offset;
    OutputStream *object_stream;

    object_table objects;
};

/* A SegmentStore, as the name suggests, is used to store the contents of many
 * segments.  The SegmentStore internally tracks where data should be placed
 * (such as a local directory or remote storage), and allows new segments to be
 * easily created as needed. */
class SegmentStore {
public:
    // New segments will be stored in the given directory.
    SegmentStore(const std::string &path);

    SegmentWriter *new_segment();

private:
    std::string directory;
};

#endif // _LBS_STORE_H
