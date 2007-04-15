/* LBS: An LFS-inspired filesystem backup system
 * Copyright (C) 2006  Michael Vrable
 *
 * Backup data is stored in a collection of objects, which are grouped together
 * into segments for storage purposes.  This implementation of the object store
 * is built on top of libtar, and represents segments as TAR files and objects
 * as files within them. */

#ifndef _LBS_TARSTORE_H
#define _LBS_TARSTORE_H

#include <stdint.h>
#include <libtar.h>

#include <string>

#include "store.h"

class Tarfile {
public:
    Tarfile(const std::string &path, const std::string &segment);
    virtual ~Tarfile();

    void write_object(int id, const char *data, size_t len);

private:
    std::string segment_name;
    TAR *t;
};

#endif // _LBS_TARSTORE_H
