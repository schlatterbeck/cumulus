/* LBS: An LFS-inspired filesystem backup system
 * Copyright (C) 2006  Michael Vrable
 *
 * Backup data is stored in a collection of objects, which are grouped together
 * into segments for storage purposes.  This file provides interfaces for
 * reading and writing objects and segments. */

#include <assert.h>
#include <uuid/uuid.h>

#include "store.h"

using std::string;

OutputStream::OutputStream()
    : bytes_written(0)
{
}

void OutputStream::write(const void *data, size_t len)
{
    write_internal(data, len);
    bytes_written += len;
}

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

void StringOutputStream::write_internal(const void *data, size_t len)
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

void FileOutputStream::write_internal(const void *data, size_t len)
{
    size_t res;

    res = fwrite(data, 1, len, f);
    if (res != len) {
        throw IOException("write error");
    }
}

WrapperOutputStream::WrapperOutputStream(OutputStream &o)
    : real(o)
{
}

void WrapperOutputStream::write_internal(const void *data, size_t len)
{
    real.write(data, len);
}

/* Provide checksumming of a data stream. */
ChecksumOutputStream::ChecksumOutputStream(OutputStream &o)
    : real(o)
{
}

void ChecksumOutputStream::write_internal(const void *data, size_t len)
{
    real.write(data, len);
    csum.process(data, len);
}

const uint8_t *ChecksumOutputStream::finish_and_checksum()
{
    return csum.checksum();
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

SegmentWriter::SegmentWriter(OutputStream *output, struct uuid u)
    : raw_out(output),
      id(u),
      object_stream(NULL)
{
    /* All output data will be checksummed except the very last few bytes,
     * which are the checksum itself. */
    out = new ChecksumOutputStream(*raw_out);

    /* Write out the segment header first. */
    static const char signature[] = "LBSSEG0\n";
    out->write(signature, strlen(signature));
    out->write(id.bytes, sizeof(struct uuid));
}

SegmentWriter::~SegmentWriter()
{
    if (object_stream)
        finish_object();

    // Write out the object table which gives the sizes and locations of all
    // objects, and then add the trailing signature, which indicates the end of
    // the segment and gives the offset of the object table.
    int64_t index_offset = out->get_pos();

    for (object_table::const_iterator i = objects.begin();
         i != objects.end(); ++i) {
        out->write_s64(i->first);
        out->write_s64(i->second);
    }

    static const char signature2[] = "LBSEND";
    out->write(signature2, strlen(signature2));
    out->write_s64(index_offset);
    out->write_u32(objects.size());

    /* Finally, append a checksum to the end of the file, so that its integrity
     * (against accidental, not malicious, corruption) can be verified. */
    const uint8_t *csum = out->finish_and_checksum();
    raw_out->write(csum, out->checksum_size());

    /* The SegmentWriter takes ownership of the OutputStream it is writing to,
     * and destroys it automatically when done with the segment. */
    delete out;
    delete raw_out;
}

OutputStream *SegmentWriter::new_object()
{
    if (object_stream)
        finish_object();

    object_start_offset = out->get_pos();
    object_stream = new WrapperOutputStream(*out);

    return object_stream;
}

void SegmentWriter::finish_object()
{
    assert(object_stream != NULL);

    // store (start, length) information for locating this object
    objects.push_back(std::make_pair(object_start_offset,
                                     object_stream->get_pos()));

    delete object_stream;
    object_stream = NULL;
}

struct uuid SegmentWriter::generate_uuid()
{
    struct uuid u;

    uuid_generate(u.bytes);

    return u;
}

string SegmentWriter::format_uuid(const struct uuid u)
{
    // A UUID only takes 36 bytes, plus the trailing '\0', so this is safe.
    char buf[40];

    uuid_unparse_lower(u.bytes, buf);

    return string(buf);
}

SegmentStore::SegmentStore(const string &path)
    : directory(path)
{
}

SegmentWriter *SegmentStore::new_segment()
{
    struct uuid id = SegmentWriter::generate_uuid();
    string filename = directory + "/" + SegmentWriter::format_uuid(id);

    FILE *f = fopen(filename.c_str(), "wb");
    if (f == NULL)
        throw IOException("Unable to open new segment");

    return new SegmentWriter(new FileOutputStream(f), id);
}
