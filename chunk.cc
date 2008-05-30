/* Cumulus: Smart Filesystem Backup to Dumb Servers
 *
 * Copyright (C) 2006-2008  The Regents of the University of California
 * Written by Michael Vrable <mvrable@cs.ucsd.edu>
 *
 * Much of the code in this file is taken from LBFS, which is
 * Copyright (C) 1998, 1999 David Mazieres (dm@uun.org)
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
 * blocks in a content-sensitive manner (using Rabin fingerprints).  This code
 * is largely taken from LBFS, primarily the files:
 *   liblbfs/fingerprint.C  (fingerprint.C,v 1.1 2001/01/29 22:49:13 benjie Exp)
 *   liblbfs/rabinpoly.h  (rabinpoly.h,v 1.4 2002/01/07 21:30:21 athicha Exp)
 *   liblbfs/rabinpoly.C  (rabinpoly.C,v 1.1 2001/01/29 22:49:13 benjie Exp)
 *   async/msb.h  (msb.h,v 1.6 1998/12/26 18:21:51 dm Exp)
 *   async/msb.C  (msb.C,v 1.4 1998/12/26 18:21:51 dm Exp)
 * but adapted and slimmed down to fit within Cumulus. */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <string>

#include "chunk.h"

using std::string;

// Functions/data only needed internally go in a separate namespace.  Public
// interfaces (at the end of the file) are in the global namespace.
namespace {

#define FINGERPRINT_PT  0xbfe6b8a5bf378d83LL
#define BREAKMARK_VALUE 0x78
#define MIN_CHUNK_SIZE  2048
#define MAX_CHUNK_SIZE  65535
#define TARGET_CHUNK_SIZE  4096

#define SFS_DEV_RANDOM "/dev/random"

#define INT64(n) n##LL
#define MSB64 INT64(0x8000000000000000)

template<class R> inline R
implicit_cast (R r)
{
  return r;
}

/* Highest bit set in a byte */
static const char bytemsb[0x100] = {
  0, 1, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 5,
  5, 5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
  6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 7, 7, 7, 7, 7, 7, 7, 7,
  7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
  7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
  7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
};

/* Find last set (most significant bit) */
static inline u_int fls32 (uint32_t) __attribute__ ((const));
static inline u_int
fls32 (u_int32_t v)
{
  if (v & 0xffff0000) {
    if (v & 0xff000000)
      return 24 + bytemsb[v>>24];
    else
      return 16 + bytemsb[v>>16];
  }
  if (v & 0x0000ff00)
    return 8 + bytemsb[v>>8];
  else
    return bytemsb[v];
}

static inline u_int fls64 (u_int64_t) __attribute__ ((const));
static inline u_int
fls64 (u_int64_t v)
{
  u_int32_t h;
  if ((h = v >> 32))
    return 32 + fls32 (h);
  else
    return fls32 ((u_int32_t) v);
}

static uint64_t
polymod (uint64_t nh, uint64_t nl, uint64_t d)
{
  assert (d);
  int k = fls64 (d) - 1;
  d <<= 63 - k;

  if (nh) {
    if (nh & MSB64)
      nh ^= d;
    for (int i = 62; i >= 0; i--)
      if (nh & INT64 (1) << i) {
        nh ^= d >> 63 - i;
        nl ^= d << i + 1;
      }
  }
  for (int i = 63; i >= k; i--)
    if (nl & INT64 (1) << i)
      nl ^= d >> 63 - i;
  return nl;
}

static void
polymult (uint64_t *php, uint64_t *plp, uint64_t x, uint64_t y)
{
  uint64_t ph = 0, pl = 0;
  if (x & 1)
    pl = y;
  for (int i = 1; i < 64; i++)
    if (x & (INT64 (1) << i)) {
      ph ^= y >> (64 - i);
      pl ^= y << i;
    }
  if (php)
    *php = ph;
  if (plp)
    *plp = pl;
}

static uint64_t
polymmult (uint64_t x, uint64_t y, uint64_t d)
{
  uint64_t h, l;
  polymult (&h, &l, x, y);
  return polymod (h, l, d);
}

#if 0
static uint64_t
polygcd (uint64_t x, uint64_t y)
{
  for (;;) {
    if (!y)
      return x;
    x = polymod (0, x, y);
    if (!x)
      return y;
    y = polymod (0, y, x);
  }
}

static bool
polyirreducible (uint64_t f)
{
  uint64_t u = 2;
  int m = (fls64 (f) - 1) >> 1;
  for (int i = 0; i < m; i++) {
    u = polymmult (u, u, f);
    if (polygcd (f, u ^ 2) != 1)
      return false;
  }
  return true;
}

static uint64_t
polygen (u_int degree)
{
  assert (degree > 0 && degree < 64);
  uint64_t msb = INT64 (1) << degree;
  uint64_t mask = msb - 1;
  uint64_t f;
  int rfd = open (SFS_DEV_RANDOM, O_RDONLY);
  if (rfd < 0) {
    fprintf (stderr, "%s: %m\n", SFS_DEV_RANDOM);
    exit(1);
  }
  do {
    if (read (rfd, &f, sizeof (f)) != implicit_cast<ssize_t> (sizeof (f))) {
      fprintf (stderr, "%s: read failed\n", SFS_DEV_RANDOM);
      exit(1);
    }
    f = (f & mask) | msb;
  } while (!polyirreducible (f));
  close (rfd);
  return f;
}
#endif

class rabinpoly {
  int shift;
  uint64_t T[256];              // Lookup table for mod
  void calcT ();
public:
  const uint64_t poly;          // Actual polynomial

  explicit rabinpoly (uint64_t poly);
  uint64_t append8 (uint64_t p, uint8_t m) const
    { return ((p << 8) | m) ^ T[p >> shift]; }
};

void
rabinpoly::calcT ()
{
  assert (poly >= 0x100);
  int xshift = fls64 (poly) - 1;
  shift = xshift - 8;
  uint64_t T1 = polymod (0, INT64 (1) << xshift, poly);
  for (int j = 0; j < 256; j++)
    T[j] = polymmult (j, T1, poly) | ((uint64_t) j << xshift);
}

rabinpoly::rabinpoly (uint64_t p)
  : poly (p)
{
  calcT ();
}

class window : public rabinpoly {
public:
  enum {size = 48};
  //enum {size = 24};
private:
  uint64_t fingerprint;
  int bufpos;
  uint64_t U[256];
  uint8_t buf[size];

public:
  window (uint64_t poly);
  uint64_t slide8 (uint8_t m) {
    if (++bufpos >= size)
      bufpos = 0;
    uint8_t om = buf[bufpos];
    buf[bufpos] = m;
    return fingerprint = append8 (fingerprint ^ U[om], m);
  }
  void reset () {
    fingerprint = 0;
    bzero (buf, sizeof (buf));
  }
};

window::window (uint64_t poly)
  : rabinpoly (poly), fingerprint (0), bufpos (-1)
{
  uint64_t sizeshift = 1;
  for (int i = 1; i < size; i++)
    sizeshift = append8 (sizeshift, 0);
  for (int i = 0; i < 256; i++)
    U[i] = polymmult (i, sizeshift, poly);
  bzero (buf, sizeof (buf));
}

} // end anonymous namespace

/* Public interface to this module. */
int chunk_compute_max_num_breaks(size_t buflen)
{
    return (buflen / MIN_CHUNK_SIZE) + 1;
}

int chunk_compute_breaks(const char *buf, size_t len, size_t *breakpoints)
{
    size_t start, pos;
    window w(FINGERPRINT_PT);

    int i = 0;
    start = 0;
    for (pos = 0; pos < len; pos++) {
        uint64_t sig = w.slide8(buf[pos]);
        size_t block_len = pos - start + 1;
        if ((sig % TARGET_CHUNK_SIZE == BREAKMARK_VALUE
             && block_len >= MIN_CHUNK_SIZE) || block_len >= MAX_CHUNK_SIZE) {
            breakpoints[i] = pos;
            start = pos + 1;
            i++;
            w.reset();
        }
    }

    if (start < len) {
        breakpoints[i] = len - 1;
        i++;
    }

    return i;
}

string chunk_algorithm_name()
{
    char buf[64];
    sprintf(buf, "%s-%d", "lbfs", TARGET_CHUNK_SIZE);
    return buf;
}
