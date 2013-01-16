/* Cumulus: Efficient Filesystem Backup to the Cloud
 * Copyright (C) 2013 The Cumulus Developers
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

/* Small utility program for computing chunk breakpoints for subfile
 * signatures.  This can be used by the Python database rebuilder; while the
 * Python code can compute chunk breakpoints the C++ version runs much more
 * quickly.
 *
 * Protocol: The input is binary, consisting of a 4-byte record, giving the
 * length of a data buffer in network byte order, followed by the raw data.
 * The output is line-oriented: each line consists of whitespace-separated
 * integers giving the computed breakpoints.  An input with a specified length
 * of zero ends the computation. */

#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#include "third_party/chunk.h"

#define MAX_BUFSIZE (1 << 24)

int main(int, char **)
{
    char *buf = new char[MAX_BUFSIZE];
    size_t *breakpoints = new size_t[chunk_compute_max_num_breaks(MAX_BUFSIZE)];

    while (true) {
        int32_t blocklen;
        if (fread(&blocklen, 4, 1, stdin) != 1) {
            /* Unexpected end of input or other error. */
            return 1;
        }

        blocklen = ntohl(blocklen);
        if (blocklen == 0)
            return 0;
        if (blocklen < 0 || blocklen > MAX_BUFSIZE)
            return 1;

        if (fread(buf, 1, blocklen, stdin) != static_cast<size_t>(blocklen))
            return 1;

        int num_breakpoints = chunk_compute_breaks(buf, blocklen, breakpoints);
        for (int i = 0; i < num_breakpoints; i++) {
            printf("%zd%c", breakpoints[i],
                   i == num_breakpoints - 1 ? '\n' : ' ');
        }
        fflush(stdout);
    }
}
