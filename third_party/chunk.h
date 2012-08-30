/* Cumulus: Smart Filesystem Backup to Dumb Servers
 *
 * Copyright (C) 2006-2008  The Regents of the University of California
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

/* Compute incremental backups at a sub-file level by chopping files up into
 * blocks in a content-sensitive manner (using Rabin fingerprints). */

#ifndef _LBS_CHUNK_H
#define _LBS_CHUNK_H

#include <stdint.h>
#include <string>

/* Block breakpoints can only be computed for a single block of memory, all
 * loaded at once.  compute_breaks will, given a block of memory, compute the
 * offsets at which successive blocks should end.  These will be stored into
 * the provided memory at breakpoints.  The maximum possible number of blocks
 * (given the block size constaints) can be computed by compute_max_num_breaks
 * so that the breakpoints array can be properly sized.  The actual number of
 * blocks is returned by the compute_breaks function. */
int chunk_compute_max_num_breaks(size_t buflen);
int chunk_compute_breaks(const char *buf, size_t len, size_t *breakpoints);
std::string chunk_algorithm_name();

#endif // _LBS_CHUNK_H
