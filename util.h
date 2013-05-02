/* Cumulus: Efficient Filesystem Backup to the Cloud
 * Copyright (C) 2007 The Cumulus Developers
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

/* Date/time string formatting and parsing utility functions.  All data and
 * methods are static, so this class should not be instantiated. */
class TimeFormat {
public:
    // Abbreviated time format encoded in snapshot file names.
    static const char FORMAT_FILENAME[];
    // A timestamp, in UTC, written out in an ISO 8601 format (compatible with
    // the SQLite datetime function).
    static const char FORMAT_ISO8601[];
    // Similar to the above, but including a timezone offset.
    static const char FORMAT_LOCALTIME[];

    static std::string format(time_t timestamp, const char *format, bool utc);

    static std::string isoformat(time_t timestamp)
        { return format(timestamp, FORMAT_ISO8601, true); }

private:
    TimeFormat() { }
};

#endif // _LBS_TARSTORE_H
