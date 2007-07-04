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
