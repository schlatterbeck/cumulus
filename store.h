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

typedef std::map<std::string, std::string> dictionary;

class IOException : public std::exception {
private:
    std::string error;
public:
    explicit IOException(const std::string &err) { error = err; }
    virtual ~IOException() throw () { }
    std::string getError() const { return error; }
};

class OutputStream {
public:
    virtual ~OutputStream() { }
    virtual void write(const void *data, size_t len) = 0;

    /* Convenience functions for writing other data types.  Values are always
     * written out in little-endian order. */
    void write_u8(uint8_t val);
    void write_u16(uint16_t val);
    void write_u32(uint32_t val);
    void write_u64(uint64_t val);

    void write_s32(int32_t val) { write_u32((uint32_t)val); }
    void write_s64(int64_t val) { write_u64((uint64_t)val); }

    void write_varint(uint64_t val);

    void write_string(const std::string &s);
    void write_dictionary(const dictionary &d);
};

class StringOutputStream : public OutputStream {
private:
    std::stringstream buf;
public:
    StringOutputStream();

    virtual void write(const void *data, size_t len);
    std::string contents() const { return buf.str(); }
};

class FileOutputStream : public OutputStream {
private:
    FILE *f;
public:
    explicit FileOutputStream(FILE *file);
    virtual ~FileOutputStream();

    virtual void write(const void *data, size_t len);
};

std::string encode_u16(uint16_t val);
std::string encode_u32(uint32_t val);
std::string encode_u64(uint64_t val);

#endif // _LBS_STORE_H
