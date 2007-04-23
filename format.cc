/* LBS: An LFS-inspired filesystem backup system
 * Copyright (C) 2007  Michael Vrable
 *
 * Utility functions for converting various datatypes to text format (and
 * later, for parsing them back, perhaps).
 */

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

/* Return the string representation of an integer. */
string encode_int(long long n)
{
    char buf[64];
    sprintf(buf, "%lld", n);
    return buf;
}

/* Output a dictionary of string key/value pairs to the given output stream.
 * The format is a sequence of lines of the form "key: value". */
void dict_output(ostream &o, map<string, string> dict)
{
    for (map<string, string>::const_iterator i = dict.begin();
         i != dict.end(); ++i) {
        o << i->first << ": " << i->second << "\n";
    }
}
