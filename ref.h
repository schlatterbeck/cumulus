/* LBS: An LFS-inspired filesystem backup system
 * Copyright (C) 2007  Michael Vrable
 *
 * Backups are structured as a collection of objects, which may refer to other
 * objects.  Object references are used to name other objects or parts of them.
 * This file defines the class for representing object references and the
 * textual representation of these references. */

#ifndef _LBS_REF_H
#define _LBS_REF_H

#include <string>

/* ======================== Object Reference Syntax ========================
 *
 * Segments are groups of objects.  Segments are named by UUID, which is a
 * 128-bit value.  The text representation is
 *    <segment> ::= xxxxxxxx-xxxx-xxxx-xxxxxxxxxxxxxxxxx
 * where each <x> is a lowercase hexadecimal digit ([0-9a-f]).
 *
 * Each object within a segment is assigned a sequence number, which is given
 * as a hexadecimal value:
 *    <object-seq> ::= xxxxxxxx
 *
 * An object can be uniquely named by the combination of segment name and
 * object sequence number:
 *    <object-name> ::= <segment> "/" <object-seq>
 * Example: "cf47429e-a503-43ac-9c31-bb3175fbb820/0000002b"
 *
 * An object name may optionally be suffixed with a checksum, which allows
 * checking the integrity of the referenced object.
 *    <checksum> ::= <checksum-alg> "=" <checksum-value>
 * Currently the only checksum-alg is "sha1", but others may be defined later.
 * <checksum-value> is a hexadecimal string.  If included, the checksum is
 * enclosed in parentheses.
 *
 * Each object is stored as a string of bytes, and object reference may specify
 * a substring rather than the entire string using a range specifier.  If no
 * range specifier is given, then by default the entire object is used.
 *    <range> ::= <start> "+" <length>
 * Both <start> and <length> are decimal values.  If included, the range is
 * enclosed in brackets.  As an abbreviation, if <start> is 0 then the range
 * can be given as just <length> (no "+" needed).
 *
 * When both a checksum and a range are included, note that the checksum is
 * taken over the entire original object, before the range is taken into
 * account.
 *
 * The full syntax for an object reference is:
 *    <object-reference>
 *      ::= <object-name> [ "(" <checksum> ")" ] [ "[" <range> "]" ]
 * Example: "cf47429e-a503-43ac-9c31-bb3175fbb820/0000002b(sha1=b9f5d0a21b8d07356723f041f5463dec892654af)[1024+512]"
 *
 * Finally, in specific circumstances, and indirect reference may be used.  In
 * cases where data could be listed directly, instead an object reference can
 * be given, prefixed with "@", which indicates that the data stored at the
 * referenced object should be treated as being included.
 *    <indirect-reference> ::= "@" <object-reference>
 */

/* Generate a fresh UUID, suitable for use as a segment name. */
std::string generate_uuid();

/* Class representing an object reference, which allows it to be manipulated
 * and converted to and from the text representation. */
class ObjectReference {
public:
    enum RefType { REF_NULL, REF_ZERO, REF_NORMAL };

    ObjectReference();
    ObjectReference(RefType t);
    ObjectReference(const std::string& segment, int sequence);
    ObjectReference(const std::string& segment, const std::string& sequence);

    bool is_null() const { return type == REF_NULL; }
    bool is_normal() const { return type == REF_NORMAL; }
    std::string to_string() const;
    static ObjectReference parse(const std::string& s);

    std::string get_segment() const { return segment; }
    std::string get_sequence() const { return object; }
    std::string get_basename() const { return segment + "/" + object; }

    bool has_checksum() const { return checksum_valid; }
    std::string get_checksum() const { return checksum; }
    void clear_checksum() { checksum = ""; checksum_valid = false; }
    void set_checksum(const std::string& checksum)
        { this->checksum = checksum; checksum_valid = true; }

    bool has_range() const { return range_valid; }
    size_t get_range_start() const { return range_start; }
    size_t get_range_length() const { return range_length; }
    void clear_range() { range_start = range_length = 0; range_valid = false; }
    void set_range(size_t start, size_t length)
        { range_start = start; range_length = length; range_valid = true; }

    bool merge(ObjectReference ref);

private:
    RefType type;
    std::string segment, object, checksum;
    size_t range_start, range_length;
    bool checksum_valid, range_valid;
};

#endif // _LBS_REF_H
