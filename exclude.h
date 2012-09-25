/* Cumulus: Efficient Filesystem Backup to the Cloud
 * Copyright (C) 2012 The Cumulus Developers
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

/* Include/exclude processing for selecting files to be backed up: mechanisms
 * for matching filenames against patterns and constructing lists of
 * include/exclude rules. */

#ifndef _CUMULUS_EXCLUDE_H
#define _CUMULUS_EXCLUDE_H

#include <sys/types.h>
#include <regex.h>
#include <list>
#include <map>
#include <string>

/* Base class for objects which should not have implicit copy constructors and
 * assignment operators. */
class noncopyable {
protected:
    noncopyable() { }
private:
    noncopyable(const noncopyable&);
    const noncopyable& operator=(const noncopyable&);
};

/* A pattern which can be matched against file paths while scanning the file
 * system for backups.  The pattern language is described in doc/exclude.rst.
 * */
class FilePattern : public noncopyable {
public:
    /* Constructs a FilePattern which the specified pattern.  If patterns are
     * loaded from a per-directory merge file, basedir should be the path to
     * the directory where the patterns were loaded (and the pattern will only
     * match files in or below that directory).  basedir should be empty for a
     * pattern matching starting at the root. */
    FilePattern(const std::string& pattern, const std::string& basedir);

    ~FilePattern();

    /* Reference counting for freeing FilePattern objects.  Newly created
     * objects have a reference count of 1.  If the reference count drops to
     * zero via unref(), the object is deleted. */
    void ref() { refcount++; }
    void unref() { if (--refcount == 0) delete this; }

    /* Returns the original pattern used to construct the FilePattern object;
     * this is intended primarily for logging/debugging. */
    const std::string& pattern() { return orig_pattern; }

    /* Does this pattern match the specified file?  Paths should be specified
     * without any leading slash.  A trailing slash should be included in the
     * path when the object is a directory, to indicate this (so that
     * directory-only rules can be processed properly). */
    bool matches(const std::string& path) const;

private:
    /* Compiles a Cumulus pattern to a regular expression.  This is used for
     * the underlying matching implementation. */
    static std::string pattern_to_regex(const std::string& pattern,
                                        const std::string& basedir);

    /* Quotes any special characters in the input to produce a regular
     * expression matching the literal string pattern. */
    static std::string regex_quote(const std::string& pattern);

    int refcount;  // Reference count for object lifetime management.
    std::string orig_pattern;  // Original pattern, returned by pattern()

    bool valid;  // True if regex is valid and initialized
    regex_t regex;  // The pattern, converted to a compiled regular expression
};

/* A PathFilterList represents a collection of rules for selecting files to be
 * included or excluded from a backup.  Patterns can be appended to the list,
 * and PathFilterList also supports updating the list via per-directory rule
 * files. */
class PathFilterList : public noncopyable {
public:
    PathFilterList();
    ~PathFilterList();

    /* Possible pattern types, as described in doc/exclude.rst. */
    enum PatternType { INCLUDE, EXCLUDE, DIRMERGE };

    /* During the backup, a call to save() will store a snapshot of the current
     * rule set.  After any modifications to the filter list, a call to
     * restore() will change the rules back to those from the time of the
     * snapshot.  Calls to save() and restore() can be nested; the saved
     * snapshots act as a stack. */
    void save();
    void restore();

    /* Append a new pattern to the end of the list of rules. */
    void add_pattern(PatternType type, const std::string& pattern,
                     const std::string& basedir);

    /* Should a specified file be included in the backup, according to the
     * current rules?  The first matching rule applies; if no rule matches the
     * default is to include the file.  is_directory is a boolean indicating
     * whether the path specifies a directory (so that directory-only rules can
     * be matched properly. */
    bool is_included(const std::string& path, bool is_directory) const;

    /* Does the given file match a dir-merge rule in the current rule set? */
    bool is_mergefile(const std::string& path) const;

    /* Updates the current rule set from the contents of a per-directory merge
     * file.  If is_mergefile returns true, then call merge_patterns specifying
     * the path to the merge file once again, the base directory containing the
     * merge file (which is the starting point for matching the new rules), and
     * the contents of the merge file as an in-memory string. */
    void merge_patterns(const std::string& path, const std::string& basedir,
                        const std::string& contents);

private:
    /* A particular set of rules is stored simply as an ordered list of (rule
     * type, pattern) tuples.  Lifetime of the FilePattern objects is managed
     * with reference counts. */
    typedef std::list<std::pair<PatternType, FilePattern *> > PatternList;

    /* A stack of patterns, for handling save()/restore() calls.  The current
     * set of rules appears at the head of the list.  As an optimization to
     * better support save()/restore() calls without any modification to the
     * rules, the stack uses run-length encoding: each item on the stack
     * consists of a set of rules and a count of how many times those rules
     * have been pushed. */
    std::list<std::pair<int, PatternList *> > pattern_stack;

    /* Parses rules (such as those in a per-directory merge file) and returns a
     * PatternList.  basedir should be the directory where the files were
     * parsed from (all rules will be matched relative to this directory), and
     * the contents of the rules file should be read in and passed as rules. */
    static PatternList *parse_rules(const std::string& basedir,
                                    const std::string& rules);

    /* Returns the current set of rules (from the head of pattern_stack). */
    const PatternList &patterns() const {
        return *pattern_stack.front().second;
    }

    /* Returns a copy of the current rules, suitable for modification.  If the
     * current head of pattern_stack has a repetition greater than 1, an
     * unshared copy of the current rule set is created. */
    PatternList *mutable_patterns();
};

#endif // _CUMULUS_EXCLUDE_H
