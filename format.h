/* LBS: An LFS-inspired filesystem backup system
 * Copyright (C) 2006  Michael Vrable
 *
 * Utility functions for converting various datatypes to text format (and
 * later, for parsing them back, perhaps).
 */

#ifndef _LBS_FORMAT_H
#define _LBS_FORMAT_H

#include <string>

std::string uri_encode(const std::string &in);

#endif // _LBS_TARSTORE_H
