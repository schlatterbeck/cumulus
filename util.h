/* LBS: An LFS-inspired filesystem backup system
 * Copyright (C) 2006  Michael Vrable
 *
 * Utility functions for converting various datatypes to text format (and
 * later, for parsing them back, perhaps).
 */

#ifndef _LBS_FORMAT_H
#define _LBS_FORMAT_H

#include <iostream>
#include <map>
#include <string>

std::string uri_encode(const std::string &in);
std::string uri_decode(const std::string &in);
std::string encode_int(long long n, int base=10);
std::string encode_dict(const std::map<std::string, std::string>& dict);
void dict_output(std::ostream &o,
                 const std::map<std::string, std::string>& dict);

long long parse_int(const std::string &s);

#endif // _LBS_TARSTORE_H
