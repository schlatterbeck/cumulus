/* LBS: An LFS-inspired filesystem backup system
 * Copyright (C) 2007  Michael Vrable
 *
 * Backups are structured as a collection of objects, which may refer to other
 * objects.  Object references are used to name other objects or parts of them.
 * This file defines the class for representing object references and the
 * textual representation of these references. */

#include <assert.h>
#include <stdio.h>
#include <uuid/uuid.h>

#include <string>

#include "ref.h"

using std::string;

/* Generate a new UUID, and return the text representation of it.  This is
 * suitable for generating the name for a new segment. */
string generate_uuid()
{
    uint8_t uuid[16];
    char buf[40];

    uuid_generate(uuid);
    uuid_unparse_lower(uuid, buf);
    return string(buf);
}

ObjectReference::ObjectReference()
    : segment(""), object("")
{
}

ObjectReference::ObjectReference(const std::string& segment, int sequence)
    : segment(segment)
{
    char seq_buf[64];
    sprintf(seq_buf, "%08x", sequence);
    object = seq_buf;

    clear_checksum();
    clear_range();
}

ObjectReference::ObjectReference(const std::string& segment,
                                 const std::string& sequence)
    : segment(segment), object(sequence)
{
    clear_checksum();
    clear_range();
}

string ObjectReference::to_string() const
{
    string result = segment + "/" + object;

    if (checksum_valid)
        result += "(" + checksum + ")";

    if (range_valid) {
        char buf[64];
        sprintf(buf, "[%zu+%zu]", range_start, range_length);
        result += buf;
    }

    return result;
}

/* Parse a string object reference and return a pointer to a new
 * ObjectReference.  The caller is responsible for freeing the object.  NULL is
 * returned if there is an error in the syntax. */
ObjectReference *ObjectReference::parse(const std::string& str)
{
    const char *s = str.c_str();
    const char *t;

    // Segment
    t = s;
    while ((*t >= '0' && *t <= '9') || (*t >= 'a' && *t <= 'f') || (*t == '-'))
        t++;
    if (*t != '/')
        return NULL;
    string segment(s, t - s);

    // Object sequence number
    t++;
    s = t;
    while ((*t >= '0' && *t <= '9') || (*t >= 'a' && *t <= 'f'))
        t++;
    if (*t != '\0' && *t != '(' && *t != '[')
        return NULL;
    string object(s, t - s);

    // Checksum
    string checksum;
    if (*t == '(') {
        t++;
        s = t;
        while (*t != ')' && *t != '\0')
            t++;
        if (*t != ')')
            return NULL;
        checksum = string(s, t - s);
        t++;
    }

    // Range
    bool have_range = false;
    int64_t range1, range2;
    if (*t == '[') {
        t++;
        s = t;
        while (*t >= '0' && *t <= '9')
            t++;
        if (*t != '+')
            return NULL;

        string val(s, t - s);
        range1 = atoll(val.c_str());

        t++;
        s = t;
        while (*t >= '0' && *t <= '9')
            t++;
        if (*t != ']')
            return NULL;

        val = string(s, t - s);
        range2 = atoll(val.c_str());

        have_range = true;
    }

    ObjectReference *ref = new ObjectReference(segment, object);
    if (checksum.size() > 0)
        ref->set_checksum(checksum);

    if (have_range)
        ref->set_range(range1, range2);

    return ref;
}

/* Attempt to merge a new object reference into the current one.  Returns a
 * boolean indicating success; if successful this reference is modified so that
 * it refers to the range of bytes originally covered by this reference plus
 * the reference passed in.  Merging only succeeds if both references refer to
 * the same object and the byte ranges are contiguous. */
bool ObjectReference::merge(ObjectReference ref)
{
    // Exception: We can always merge into a null object
    if (is_null()) {
        *this = ref;
        return true;
    }

    if (segment != ref.segment)
        return false;
    if (object != ref.object)
        return false;

    // TODO: Allow the case where only one checksum was filled in
    if (checksum_valid != ref.checksum_valid || checksum != ref.checksum)
        return false;

    if (!range_valid || !ref.range_valid)
        return false;

    if (range_start + range_length == ref.range_start) {
        range_length += ref.range_length;
        return true;
    } else {
        return false;
    }
}
