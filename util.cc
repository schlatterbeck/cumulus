/* Cumulus: Smart Filesystem Backup to Dumb Servers
 *
 * Copyright (C) 2007  The Regents of the University of California
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

/* Utility functions for converting various datatypes to text format (and
 * later, for parsing them back, perhaps). */

#include <stdio.h>
#include <uuid/uuid.h>

#include <iostream>
#include <map>
#include <string>
#include <sstream>

using std::map;
using std::ostream;
using std::string;

/* Perform URI-style escaping of a string.  Bytes which cannot be represented
 * directly are encoded in the form %xx (where "xx" is a string of two
 * hexadecimal digits). */
string uri_encode(const string &in)
{
    string out;

    for (size_t i = 0; i < in.length(); i++) {
        unsigned char c = in[i];

        if (c >= '+' && c < 0x7f && c != '@') {
            out += c;
        } else {
            char buf[4];
            sprintf(buf, "%%%02x", c);
            out += buf;
        }
    }

    return out;
}

/* Decoding of strings produced by uri_encode. */
string uri_decode(const string &in)
{
    char *buf = new char[in.size() + 1];

    const char *input = in.c_str();
    char *output = buf;

    while (*input != '\0') {
        if (*input == '%') {
            char hexbuf[4];
            if (isxdigit(input[1]) && isxdigit(input[2])) {
                hexbuf[0] = input[1];
                hexbuf[1] = input[2];
                hexbuf[2] = '\0';
                *output++ = strtol(hexbuf, NULL, 16);
                input += 3;
            } else {
                input++;
            }
        } else {
            *output++ = *input++;
        }
    }

    *output = '\0';

    string result(buf);
    delete[] buf;
    return result;
}

/* Return the string representation of an integer.  Will try to produce output
 * in decimal, hexadecimal, or octal according to base, though this is just
 * advisory.  For negative numbers, will always use decimal. */
string encode_int(long long n, int base)
{
    char buf[64];

    if (n >= 0 && base == 16) {
        sprintf(buf, "0x%llx", n);
        return buf;
    }

    if (n > 0 && base == 8) {
        sprintf(buf, "0%llo", n);
        return buf;
    }

    sprintf(buf, "%lld", n);
    return buf;
}

/* Parse the string representation of an integer.  Accepts decimal, octal, and
 * hexadecimal, just as C would (recognizes the 0 and 0x prefixes). */
long long parse_int(const string &s)
{
    return strtoll(s.c_str(), NULL, 0);
}
