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

#ifndef _LBS_FORMAT_H
#define _LBS_FORMAT_H

#include <iostream>
#include <map>
#include <string>

std::string uri_encode(const std::string &in);
std::string uri_decode(const std::string &in);
std::string encode_int(long long n, int base=10);

long long parse_int(const std::string &s);
void cloexec(int fd);

void fatal(std::string msg) __attribute__((noreturn));

#endif // _LBS_TARSTORE_H
