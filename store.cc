/* LBS: An LFS-inspired filesystem backup system
 * Copyright (C) 2006  Michael Vrable
 *
 * Backup data is stored in a collection of objects, which are grouped together
 * into segments for storage purposes.  This file provides interfaces for
 * reading and writing objects and segments. */

#include <assert.h>

#include "store.h"

using std::string;

void OutputStream::write_u8(uint8_t val)
{
    write(&val, 1);
}

void OutputStream::write_u16(uint16_t val)
{
    unsigned char buf[2];

    buf[0] = val & 0xff;
    buf[1] = (val >> 8) & 0xff;
    write(buf, 2);
}

void OutputStream::write_u32(uint32_t val)
{
    unsigned char buf[4];

    buf[0] = val & 0xff;
    buf[1] = (val >> 8) & 0xff;
    buf[2] = (val >> 16) & 0xff;
    buf[3] = (val >> 24) & 0xff;
    write(buf, 4);
}

void OutputStream::write_u64(uint64_t val)
{
    unsigned char buf[8];

    buf[0] = val & 0xff;
    buf[1] = (val >> 8) & 0xff;
    buf[2] = (val >> 16) & 0xff;
    buf[3] = (val >> 24) & 0xff;
    buf[4] = (val >> 32) & 0xff;
    buf[5] = (val >> 40) & 0xff;
    buf[6] = (val >> 48) & 0xff;
    buf[7] = (val >> 56) & 0xff;
    write(buf, 8);
}

/* Writes an integer to an output stream using a variable-sized representation:
 * seven bits are written at a time (little-endian), and the eigth bit of each
 * byte is set if more data follows. */
void OutputStream::write_varint(uint64_t val)
{
    do {
        uint8_t remainder = (val & 0x7f);
        val >>= 7;
        if (val)
            remainder |= 0x80;
        write_u8(remainder);
    } while (val);
}

/* Write an arbitrary string by first writing out the length, followed by the
 * data itself. */
void OutputStream::write_string(const string &s)
{
    size_t len = s.length();
    write_varint(len);
    write(s.data(), len);
}

void OutputStream::write_dictionary(const dictionary &d)
{
    size_t size = d.size();
    size_t written = 0;

    write_varint(size);

    for (dictionary::const_iterator i = d.begin(); i != d.end(); ++i) {
        write_string(i->first);
        write_string(i->second);
        written++;
    }

    assert(written == size);
}

StringOutputStream::StringOutputStream()
    : buf(std::ios_base::out)
{
}

void StringOutputStream::write(const void *data, size_t len)
{
    buf.write((const char *)data, len);
    if (!buf.good())
        throw IOException("error writing to StringOutputStream");
}

FileOutputStream::FileOutputStream(FILE *file)
{
    f = file;
}

FileOutputStream::~FileOutputStream()
{
    fclose(f);
}

void FileOutputStream::write(const void *data, size_t len)
{
    size_t res;

    res = fwrite(data, 1, len, f);
    if (res != len) {
        throw IOException("write error");
    }
}

/* Utility functions, for encoding data types to strings. */
string encode_u16(uint16_t val)
{
    StringOutputStream s;
    s.write_u16(val);
    return s.contents();
}

string encode_u32(uint32_t val)
{
    StringOutputStream s;
    s.write_u32(val);
    return s.contents();
}

string encode_u64(uint64_t val)
{
    StringOutputStream s;
    s.write_u64(val);
    return s.contents();
}
