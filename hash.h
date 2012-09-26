/* Cumulus: Smart Filesystem Backup to Dumb Servers
 *
 * Copyright (C) 2012  Michael Vrable <vrable@cs.hmc.edu>
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

/* A generic interface for computing digests of data, used for both
 * content-based deduplication and for data integrity verification. */

#ifndef CUMULUS_HASH_H
#define CUMULUS_HASH_H 1

#include <stdint.h>
#include <string>

/* An object-oriented wrapper around checksumming functionality. */
class Hash {
public:
    Hash() : digest_bytes(NULL) { }
    virtual ~Hash() { }

    // 
    virtual void update(const void *data, size_t len) = 0;
    // Returns the size of the buffer returned by digest, in bytes.
    virtual size_t digest_size() const = 0;
    // Returns the name of the hash algorithm.
    virtual std::string name() const = 0;

    // Calls update with the contents of the data found in the specified file.
    bool update_from_file(const char *filename);
    // Finalizes the digest and returns a pointer to a raw byte array
    // containing the hash.
    const uint8_t *digest();

    // Returns the digest in text form: "<digest name>=<hex digits>".
    std::string digest_str();

    //typedef Hash *(*HashConstructor)();
    static void Register(const std::string& name, Hash *(*constructor)());
    static Hash *New();
    static Hash *New(const std::string& name);

protected:
    virtual const uint8_t *finalize() = 0;

private:
    const uint8_t *digest_bytes;
};

void hash_init();

#endif
