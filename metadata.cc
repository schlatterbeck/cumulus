/* Cumulus: Efficient Filesystem Backup to the Cloud
 * Copyright (C) 2007-2008 The Cumulus Developers
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

/* Handling of metadata written to backup snapshots.  This manages the writing
 * of file metadata into new backup snapshots, including breaking the metadata
 * log apart across separate objects.  Eventually this should include unified
 * handling of the statcache, and re-use of metadata between snapshots.
 */

#include <stdlib.h>
#include <string.h>
#include <sys/sysmacros.h>
#include <string>
#include <iostream>
#include <map>

#include "metadata.h"
#include "localdb.h"
#include "ref.h"
#include "store.h"
#include "util.h"

using std::list;
using std::map;
using std::string;
using std::ostream;
using std::ostringstream;

static const size_t LBS_METADATA_BLOCK_SIZE = 65536;

// If true, forces a full write of metadata: will not include pointers to
// metadata in old snapshots.
bool flag_full_metadata = false;

/* TODO: Move to header file */
extern LocalDb *db;

/* Like strcmp, but sorts in the order that files will be visited in the
 * filesystem.  That is, we break paths apart at slashes, and compare path
 * components separately. */
static int pathcmp(const char *path1, const char *path2)
{
    /* Find the first component in each path. */
    const char *slash1 = strchr(path1, '/');
    const char *slash2 = strchr(path2, '/');

    {
        string comp1, comp2;
        if (slash1 == NULL)
            comp1 = path1;
        else
            comp1 = string(path1, slash1 - path1);

        if (slash2 == NULL)
            comp2 = path2;
        else
            comp2 = string(path2, slash2 - path2);

        /* Directly compare the two components first. */
        if (comp1 < comp2)
            return -1;
        if (comp1 > comp2)
            return 1;
    }

    if (slash1 == NULL && slash2 == NULL)
        return 0;
    if (slash1 == NULL)
        return -1;
    if (slash2 == NULL)
        return 1;

    return pathcmp(slash1 + 1, slash2 + 1);
}

/* Encode a dictionary of string key/value pairs into a sequence of lines of
 * the form "key: value".  If it exists, the key "name" is treated specially
 * and will be listed first. */
static string encode_dict(const map<string, string>& dict)
{
    string result;

    if (dict.find("name") != dict.end()) {
        result += "name: " + dict.find("name")->second + "\n";
    }

    for (map<string, string>::const_iterator i = dict.begin();
         i != dict.end(); ++i) {
        if (i->first == "name")
            continue;
        result += i->first + ": " + i->second + "\n";
    }

    return result;
}

MetadataWriter::MetadataWriter(TarSegmentStore *store,
                               const char *path,
                               const char *snapshot_name,
                               const char *snapshot_scheme)
{
    statcache_path = path;
    statcache_path += "/statcache2";
    if (snapshot_scheme != NULL && strlen(snapshot_scheme) > 0)
        statcache_path = statcache_path + "-" + snapshot_scheme;
    statcache_tmp_path = statcache_path + "." + snapshot_name;

    statcache_in = fopen(statcache_path.c_str(), "r");

    statcache_out = fopen(statcache_tmp_path.c_str(), "w");
    if (statcache_out == NULL) {
        fprintf(stderr, "Error opening statcache %s: %m\n",
                statcache_tmp_path.c_str());
        fatal("Error opening statcache");
    }

    old_metadata_eof = false;

    this->store = store;
    chunk_size = 0;
}

/* Read the next entry from the old statcache file, loading it into
 * old_metadata. */
void MetadataWriter::read_statcache()
{
    if (statcache_in == NULL) {
        old_metadata_eof = true;
        return;
    }

    old_metadata.clear();

    char *buf = NULL;
    size_t n = 0;
    string field = "";          // Last field to be read in

    /* Look for a first line starting with "@@", which tells where the metadata
     * can be found in the metadata log of an old snapshot. */
    if (getline(&buf, &n, statcache_in) < 0
        || buf == NULL || buf[0] != '@' || buf[1] != '@') {
        old_metadata_eof = true;
        return;
    }

    if (strchr(buf, '\n') != NULL)
        *strchr(buf, '\n') = '\0';
    old_metadata_loc = buf + 2;

    /* After the initial line follows the metadata, as key-value pairs. */
    while (!feof(statcache_in)) {
        if (getline(&buf, &n, statcache_in) < 0)
            break;

        char *eol = strchr(buf, '\n');
        if (eol != NULL)
            *eol = '\0';

        /* Is the line blank?  If so, we have reached the end of this entry. */
        if (buf[0] == '\0')
            break;

        /* Is this a continuation line?  (Does it start with whitespace?) */
        if (isspace(buf[0]) && field != "") {
            old_metadata[field] += string("\n") + buf;
            continue;
        }

        /* For lines of the form "Key: Value" look for ':' and split the line
         * apart. */
        char *value = strchr(buf, ':');
        if (value == NULL)
            continue;
        *value = '\0';
        field = buf;

        value++;
        while (isspace(*value))
            value++;

        old_metadata[field] = value;
    }

    if (feof(statcache_in) && old_metadata.size() == 0) {
        old_metadata_eof = true;
    }

    free(buf);
}

bool MetadataWriter::find(const string& path)
{
    const char *path_str = path.c_str();
    while (!old_metadata_eof) {
        string old_path = uri_decode(old_metadata["name"]);
        int cmp = pathcmp(old_path.c_str(), path_str);
        if (cmp == 0)
            return true;
        else if (cmp > 0)
            return false;
        else
            read_statcache();
    }

    return false;
}

/* Does a file appear to be unchanged from the previous time it was backed up,
 * based on stat information? */
bool MetadataWriter::is_unchanged(const struct stat *stat_buf)
{
    if (old_metadata.find("volatile") != old_metadata.end()
        && parse_int(old_metadata["volatile"]) != 0)
        return false;

    if (old_metadata.find("ctime") == old_metadata.end())
        return false;
    if (stat_buf->st_ctime != parse_int(old_metadata["ctime"]))
        return false;

    if (old_metadata.find("mtime") == old_metadata.end())
        return false;
    if (stat_buf->st_mtime != parse_int(old_metadata["mtime"]))
        return false;

    if (old_metadata.find("size") == old_metadata.end())
        return false;
    if (stat_buf->st_size != parse_int(old_metadata["size"]))
        return false;

    if (old_metadata.find("inode") == old_metadata.end())
        return false;
    string inode = encode_int(major(stat_buf->st_dev))
        + "/" + encode_int(minor(stat_buf->st_dev))
        + "/" + encode_int(stat_buf->st_ino);
    if (inode != old_metadata["inode"])
        return false;

    return true;
}

list<ObjectReference> MetadataWriter::get_blocks()
{
    list<ObjectReference> blocks;

    /* Parse the list of blocks. */
    const char *s = old_metadata["data"].c_str();
    while (*s != '\0') {
        if (isspace(*s)) {
            s++;
            continue;
        }

        string ref = "";
        while (*s != '\0' && !isspace(*s)) {
            char buf[2];
            buf[0] = *s;
            buf[1] = '\0';
            ref += buf;
            s++;
        }

        ObjectReference r = ObjectReference::parse(ref);
        if (!r.is_null())
            blocks.push_back(r);
    }

    return blocks;
}

/* Ensure contents of metadata are flushed to an object. */
void MetadataWriter::metadata_flush()
{
    int offset = 0;

    ostringstream metadata;
    ObjectReference indirect;
    for (list<MetadataItem>::iterator i = items.begin();
         i != items.end(); ++i) {
        // If indirectly referencing any other metadata logs, be sure those
        // segments are properly referenced.
        if (i->reused) {
            db->UseObject(i->ref);
        }

        // Write out an indirect reference to any previous objects which could
        // be reused
        if (!i->reused || !indirect.merge(i->ref)) {
            if (!indirect.is_null()) {
                string refstr = indirect.to_string();
                metadata << "@" << refstr << "\n";
                offset += refstr.size() + 2;
                if (!i->reused) {
                    metadata << "\n";
                    offset += 1;
                }
            }
            if (i->reused)
                indirect = i->ref;
            else
                indirect = ObjectReference();
        }

        if (!i->reused) {
            metadata << i->text;
            i->offset = offset;
            offset += i->text.size();
        }
    }
    if (!indirect.is_null()) {
        string refstr = indirect.to_string();
        metadata << "@" << refstr << "\n";
        offset += refstr.size() + 2;
        indirect = ObjectReference();
    }

    string m = metadata.str();
    if (m.size() == 0)
        return;

    /* Write current metadata information to a new object. */
    LbsObject *meta = new LbsObject;
    meta->set_group("metadata");
    meta->set_data(m.data(), m.size(), NULL);
    meta->write(store);

    /* Write a reference to this block in the root. */
    ObjectReference ref = meta->get_ref();
    metadata_root << "@" << ref.to_string() << "\n";
    db->UseObject(ref);

    delete meta;

    /* Write these files out to the statcache, and include a reference to where
     * the metadata lives (so we can re-use it if it has not changed). */
    for (list<MetadataItem>::const_iterator i = items.begin();
         i != items.end(); ++i) {
        ObjectReference r = ref;
        r.set_range(i->offset, i->text.size());

        if (i->reused)
            r = i->ref;

        string refstr = r.to_string();
        fprintf(statcache_out, "@@%s\n%s", refstr.c_str(), i->text.c_str());
    }

    chunk_size = 0;
    items.clear();
}

void MetadataWriter::add(dictionary info)
{
    MetadataItem item;
    item.offset = 0;
    item.reused = false;
    item.text += encode_dict(info) + "\n";

    if (info == old_metadata && !flag_full_metadata) {
        ObjectReference ref = ObjectReference::parse(old_metadata_loc);
        if (!ref.is_null() && db->IsAvailable(ref)) {
            item.reused = true;
            item.ref = ref;
        }
    }

    items.push_back(item);
    chunk_size += item.text.size();

    if (chunk_size > LBS_METADATA_BLOCK_SIZE)
        metadata_flush();
}

ObjectReference MetadataWriter::close()
{
    metadata_flush();
    const string root_data = metadata_root.str();

    LbsObject *root = new LbsObject;
    root->set_group("metadata");
    root->set_data(root_data.data(), root_data.size(), NULL);
    root->write(store);
    db->UseObject(root->get_ref());

    ObjectReference ref = root->get_ref();
    delete root;

    fclose(statcache_out);
    if (rename(statcache_tmp_path.c_str(), statcache_path.c_str()) < 0) {
        fprintf(stderr, "Error renaming statcache from %s to %s: %m\n",
                statcache_tmp_path.c_str(), statcache_path.c_str());
    }

    return ref;
}
