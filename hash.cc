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

#include <stdio.h>
#include <stdint.h>
#include <map>
#include <string>

#include "hash.h"

using std::map;
using std::string;

static string default_algorithm;
static map<string, Hash *(*)()> hash_registry;

void Hash::Register(const std::string& name, Hash *(*constructor)())
{
    printf("Registered hash algorithm %s\n", name.c_str());
    hash_registry.insert(make_pair(name, constructor));
}

Hash *Hash::New()
{
    return New(default_algorithm);
}

Hash *Hash::New(const std::string& name)
{
    Hash *(*constructor)() = hash_registry[name];
    if (!constructor)
        return NULL;
    else
        return constructor();
}

std::string Hash::hash_file(const char *filename)
{
    string result;
    Hash *hash = Hash::New();
    if (hash->update_from_file(filename))
        result = hash->digest_str();

    delete hash;
    return result;
}

bool Hash::update_from_file(const char *filename)
{
    FILE *f = fopen(filename, "rb");
    if (f == NULL)
        return false;

    while (!feof(f)) {
        char buf[4096];
        size_t bytes = fread(buf, 1, sizeof(buf), f);

        if (ferror(f)) {
            fclose(f);
            return false;
        }

        update(buf, bytes);
    }

    fclose(f);
    return true;
}

const uint8_t *Hash::digest()
{
    if (!digest_bytes) {
        digest_bytes = finalize();
    }

    return digest_bytes;
}

string Hash::digest_str()
{
    const uint8_t *raw_digest = digest();
    size_t len = digest_size();
    char hexbuf[len*2 + 1];

    hexbuf[0] = '\0';
    for (size_t i = 0; i < len; i++) {
        snprintf(&hexbuf[2*i], 3, "%02x", raw_digest[i]);
    }

    return name() + "=" + hexbuf;
}

void sha1_register();
void sha256_register();

void hash_init()
{
    sha1_register();
    sha256_register();
    default_algorithm = "sha224";
}
