/* LBS: An LFS-inspired filesystem backup system
 * Copyright (C) 2007  Michael Vrable
 *
 * Utility functions for converting various datatypes to text format (and
 * later, for parsing them back, perhaps).
 */

#include <stdio.h>
#include <uuid/uuid.h>

#include <string>
#include <sstream>

using std::string;

/* Perform URI-style escaping of a string.  Bytes which cannot be represented
 * directly are encoded in the form %xx (where "xx" is a string of two
 * hexadecimal digits). */
string uri_encode(const string &in)
{
    string out;

    for (size_t i = 0; i < in.length(); i++) {
        unsigned char c = in[i];

        if (c > '%' && c <= 0x7f) {
            out += c;
        } else {
            char buf[4];
            sprintf(buf, "%%%02x", c);
            out += buf;
        }
    }

    return out;
}
