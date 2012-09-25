/* Cumulus: Efficient Filesystem Backup to the Cloud
 * Copyright (C) 2007-2008 The Cumulus Developers
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

/* Backups are structured as a collection of objects, which may refer to other
 * objects.  Object references are used to name other objects or parts of them.
 * This file defines the class for representing object references and the
 * textual representation of these references. */

#include <assert.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
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
    : type(REF_NULL), segment(""), object("")
{
    clear_checksum();
    clear_range();
}

ObjectReference::ObjectReference(RefType t)
    : type(t), segment(""), object("")
{
    clear_checksum();
    clear_range();
}

ObjectReference::ObjectReference(const std::string& segment, int sequence)
    : type(REF_NORMAL), segment(segment)
{
    char seq_buf[64];
    sprintf(seq_buf, "%08x", sequence);
    object = seq_buf;

    clear_checksum();
    clear_range();
}

ObjectReference::ObjectReference(const std::string& segment,
                                 const std::string& sequence)
    : type(REF_NORMAL), segment(segment), object(sequence)
{
    clear_checksum();
    clear_range();
}

string ObjectReference::to_string() const
{
    if (type == REF_NULL)
        return "null";

    string result;
    if (type == REF_ZERO) {
        result = "zero";
    } else if (type == REF_NORMAL) {
        result = segment + "/" + object;

        if (checksum_valid)
            result += "(" + checksum + ")";
    }

    if (range_valid) {
        char buf[64];
        if (range_exact) {
            sprintf(buf, "[=%zu]", range_length);
        } else if (type == REF_ZERO) {
            sprintf(buf, "[%zu]", range_length);
        } else {
            sprintf(buf, "[%zu+%zu]", range_start, range_length);
        }
        result += buf;
    }

    return result;
}

/* Parse a string object reference and return a pointer to a new
 * ObjectReference.  The caller is responsible for freeing the object.  NULL is
 * returned if there is an error in the syntax. */
ObjectReference ObjectReference::parse(const std::string& str)
{
    const char *s = str.c_str();
    const char *t;
    ObjectReference::RefType type = ObjectReference::REF_NORMAL;

    // Special case: explicit zero objects
    if (strncmp(s, "zero", 4) == 0) {
        type = ObjectReference::REF_ZERO;
        s += 4;
    }

    // Segment
    t = s;
    if (type == ObjectReference::REF_NORMAL) {
        while ((*t >= '0' && *t <= '9') || (*t >= 'a' && *t <= 'f')
               || (*t == '-'))
            t++;
        if (*t != '/')
            return ObjectReference();
    }
    string segment(s, t - s);

    // Object sequence number
    if (type == ObjectReference::REF_NORMAL) {
        t++;
        s = t;
        while ((*t >= '0' && *t <= '9') || (*t >= 'a' && *t <= 'f'))
            t++;
        if (*t != '\0' && *t != '(' && *t != '[')
            return ObjectReference();
    }
    string object(s, t - s);

    // Checksum
    string checksum;
    if (*t == '(') {
        t++;
        s = t;
        while (*t != ')' && *t != '\0')
            t++;
        if (*t != ')')
            return ObjectReference();
        checksum = string(s, t - s);
        t++;
    }

    // Range
    bool have_range = false, range_exact = false;
    int64_t range1 = 0, range2 = 0;
    if (*t == '[') {
        t++;

        if (*t == '=') {
            range_exact = true;
            t++;
        }

        s = t;
        while (*t >= '0' && *t <= '9')
            t++;

        // Abbreviated-length only range?
        if (*t == ']') {
            string val(s, t - s);
            range2 = atoll(val.c_str());
        } else {
            if (*t != '+')
                return ObjectReference();
            if (range_exact)
                return ObjectReference();

            string val(s, t - s);
            range1 = atoll(val.c_str());

            t++;
            s = t;
            while (*t >= '0' && *t <= '9')
                t++;
            if (*t != ']')
                return ObjectReference();

            val = string(s, t - s);
            range2 = atoll(val.c_str());
        }

        have_range = true;
    }

    ObjectReference ref;
    switch (type) {
    case ObjectReference::REF_ZERO:
        ref = ObjectReference(ObjectReference::REF_ZERO);
        break;
    case ObjectReference::REF_NORMAL:
        ref = ObjectReference(segment, object);
        break;
    default:
        return ObjectReference();
    }

    if (checksum.size() > 0)
        ref.set_checksum(checksum);

    if (have_range)
        ref.set_range(range1, range2, range_exact);

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

    if (range_exact || ref.range_exact)
        return false;

    if (range_start + range_length == ref.range_start) {
        range_length += ref.range_length;
        return true;
    } else {
        return false;
    }
}
