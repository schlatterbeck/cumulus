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

/* Implementation of Cumulus include/exclude rules for selecting files to be
 * backed up. */

#include <regex.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <iostream>
#include <sstream>
#include <string>

#include "exclude.h"

using std::make_pair;
using std::pair;
using std::string;

FilePattern::FilePattern(const string& pattern, const string& basedir)
    : refcount(1), orig_pattern(pattern), valid(false)
{
    string pattern_as_regex = pattern_to_regex(pattern, basedir);
    int status = regcomp(&regex, pattern_as_regex.c_str(),
                         REG_EXTENDED|REG_NOSUB);
    if (status != 0) {
        char errbuf[256];
        regerror(status, &regex, errbuf, sizeof(errbuf));
        fprintf(stderr,
                "Pattern %s: failed to compile as regular expression %s: %s\n",
                pattern.c_str(), pattern_as_regex.c_str(), errbuf);
        regfree(&regex);
    } else {
        valid = true;
    }
}

FilePattern::~FilePattern()
{
    if (valid)
        regfree(&regex);
}

bool FilePattern::matches(const std::string& path) const
{
    if (!valid)
        return false;
    else
        return regexec(&regex, path.c_str(), 0, NULL, 0) == 0;
}

string FilePattern::pattern_to_regex(const string& pattern,
                                     const string& basedir)
{
    /* Matches are always anchored to cover the entire string; we insert
     * wildcards where needed if we only need to match a suffix of the path. */
    string result = "^";
    size_t i = 0;
    size_t len = pattern.size();
    if (len == 0) {
        /* Special case: an empty pattern matches all files. */
        return result;
    }

    /* For a non-empty basedir, the match must ensure that the file actually
     * falls within basedir. */
    if (!basedir.empty() && basedir != ".") {
        result += regex_quote(basedir) + "/";
    }

    /* A leading slash indicates a pattern that must match the entire path.  If
     * there is no leading slash, match any number of leading directory
     * components. */
    if (pattern[0] == '/') {
        i++;
    } else {
        result += "(|.*/)";
    }

    while (i < len) {
        switch (pattern[i]) {
        /* Characters that must be quoted in a regular expression that are not
         * otherwise part of the Cumulus pattern language. */
        case '^':
        case '.':
        case '[':
        case ']':
        case '$':
        case '(':
        case ')':
        case '|':
        case '+':
        case '{':
        case '}':
        case '\\':
            result += '\\';
            result += pattern[i];
            break;

        case '?':
            /* Any character except a directory separator. */
            result += "[^/]";
            break;

        case '*':
            if (i + 1 < len && pattern[i + 1] == '*') {
                /* Any number of characters, including slashes. */
                i++;
                result += ".*";
            } else {
                /* Zero or more characters (but no directory separators). */
                result += "[^/]*";
            }
            break;

        default:
            /* A character matched literally that does not require quoting. */
            result += pattern[i];
            break;
        }
        i++;
    }

    /* A trailing slash should match only a directory.  No trailing slash means
     * match any type of file.  Callers should include a slash at the end of a
     * path that is a directory; if there is no trailing slash in the pattern
     * match either a trailing slash or none. */
    if (pattern[len - 1] != '/') {
        result += "/?";
    }

    result += "$";

    return result;
}

string FilePattern::regex_quote(const string& pattern)
{
    string result = "";
    for (size_t i = 0; i < pattern.length(); i++) {
        switch (pattern[i]) {
        /* Characters that must be quoted in a regular expression. */
        case '^':
        case '.':
        case '[':
        case ']':
        case '$':
        case '(':
        case ')':
        case '|':
        case '*':
        case '+':
        case '?':
        case '{':
        case '}':
        case '\\':
            result += '\\';
            // fall through

        default:
            result += pattern[i];
        }
    }

    return result;
}

PathFilterList::PathFilterList()
{
    /* Invariant: pattern_stack is always non-empty (except when the destructor
     * runs).  Thus, reading pattern_stack.front() is always safe. */
    pattern_stack.push_back(make_pair(1, new PatternList));
}

PathFilterList::~PathFilterList()
{
    /* Pops all items off the saved rule stack.  As an optimization, rather
     * than repeatedly popping items which have a repeat count, just set the
     * repeat count to one. */
    while (!pattern_stack.empty()) {
        pattern_stack.front().first = 1;
        restore();
    }
}

/* save() operates lazily: simply increment the repeat count on the rule set at
 * the head of the list.  If modifications are made, mutable_patterns() will
 * create a copy of the rules. */
void PathFilterList::save()
{
    pattern_stack.front().first++;
}

void PathFilterList::restore()
{
    if (--pattern_stack.front().first == 0) {
        PatternList *old_patterns = pattern_stack.front().second;
        pattern_stack.pop_front();
        for (PatternList::iterator i = old_patterns->begin();
             i != old_patterns->end(); ++i) {
            i->second->unref();
        }
        delete old_patterns;
    }
}

void PathFilterList::add_pattern(PatternType type, const string& pattern,
                                 const string& basedir)
{
    FilePattern *pat = new FilePattern(pattern, basedir);
    mutable_patterns()->push_back(make_pair(type, pat));
}

bool PathFilterList::is_included(const std::string& path,
                                 bool is_directory) const
{
    string full_path;
    if (is_directory) {
        full_path = path + "/";
    } else {
        full_path = path;
    }

    PatternList::const_iterator i;
    for (i = patterns().begin(); i != patterns().end(); ++i) {
        if (i->second->matches(full_path)) {
            switch (i->first) {
            case INCLUDE:
                return true;
            case EXCLUDE:
                return false;
            case DIRMERGE:
                /* Merge rules are ignored for the purposes of selecting
                 * whether a file is included or not. */
                continue;
            }
        }
    }

    /* Default is include if no rule matches. */
    return true;
}

bool PathFilterList::is_mergefile(const std::string& path) const
{
    PatternList::const_iterator i;
    for (i = patterns().begin(); i != patterns().end(); ++i) {
        if (i->first == DIRMERGE && i->second->matches(path))
            return true;
    }
    return false;
}

/* Parses the specified contents of a per-directory rule merge file.  The rules
 * are first parsed into a temporary PatternList, which is then spliced into
 * the rule set just before the DIRMERGE rule.  Thus, if a dir-merge rule
 * matches multiple times (in successive sub-directories), deeper rules take
 * precedence over earlier rules. */
void PathFilterList::merge_patterns(const string& path,
                                    const string& basedir,
                                    const string& contents)
{
    PatternList *rules = mutable_patterns();
    PatternList::iterator i;
    for (PatternList::iterator i = rules->begin(); i != rules->end(); ++i) {
        /* Try to locate where the rules should be inserted by looking for the
         * DIRMERGE rule which matches the path to the rule file. */
        if (i->first == DIRMERGE && i->second->matches(path)) {
            PatternList *new_rules = parse_rules(basedir, contents);
            rules->splice(i, *new_rules);
            delete new_rules;
            break;
        }
    }
}

PathFilterList::PatternList *PathFilterList::parse_rules(const string& basedir,
                                                         const string& data)
{
    PatternList *patterns = new PatternList;
    std::stringstream rules(data, std::stringstream::in);
    while (!rules.eof()) {
        string rule;
        std::getline(rules, rule);
        /* Ignore blank lines and lines starting with "#". */
        if (rule.empty() || rule[0] == '#')
            continue;
        if (rule.length() > 2 && rule[1] == ' ') {
            if (rule[0] == '+' || rule[0] == '-' || rule[0] == ':') {
                FilePattern *pat = new FilePattern(rule.substr(2), basedir);
                switch (rule[0]) {
                case '+':
                    patterns->push_back(make_pair(INCLUDE, pat));
                    break;
                case '-':
                    patterns->push_back(make_pair(EXCLUDE, pat));
                    break;
                case ':':
                    patterns->push_back(make_pair(DIRMERGE, pat));
                    break;
                default:
                    break;
                }
                continue;
            }
            fprintf(stderr, "Invalid rule: %s\n", rule.c_str());
        }
    }
    return patterns;
}

PathFilterList::PatternList *PathFilterList::mutable_patterns()
{
    PatternList *old_list = pattern_stack.front().second;
    if (pattern_stack.front().first == 1)
        return old_list;

    PatternList *new_list = new PatternList;
    for (PatternList::iterator i = old_list->begin();
         i != old_list->end(); ++i) {
        i->second->ref();
        new_list->push_back(*i);
    }
    pattern_stack.front().first--;
    pattern_stack.push_front(make_pair(1, new_list));
    return new_list;
}


/*****************************************************************************
 * Unit tests for pattern matching.  These are not compiled in by default, but
 * exclude.cc can be compiled to a standalone binary with -DRUN_TESTS to run
 * the unit tests.
 *****************************************************************************/

#ifdef RUN_TESTS
/* Tests of pattern matching rules.  test_pattern takes a pattern, a base
 * directory, and a path to match, and prints out whether the rule matches.
 * expect_match is the expected result; if this doesn't equal the actual result
 * print a warning message. */
static void test_pattern(const string& pattern, const string& basedir,
                         const string& path, bool expect_match)
{
    FilePattern pat(pattern, basedir);
    bool result = pat.matches(path);
    printf("%3s %c %c %-30s %-30s\n",
           result == expect_match ? "" : "ERR",
           result ? '+' : '-',
           expect_match ? '+' : '-',
           pattern.c_str(),
           path.c_str());
}

int main(int argc, char *argv[])
{
    printf("Act/Exp Pattern                        Path\n");
    test_pattern("*.o", "", "a/b/c.txt", false);
    test_pattern("*.o", "", "a/b/c.o", true);
    test_pattern("*.git/", "", "repo/project.git/", true);
    test_pattern("/.cache", "", ".cache", true);
    test_pattern("/.cache", "", "home/user/.cache", false);
    test_pattern("/*/.cache", "", "home/user/.cache", false);
    test_pattern("/*/*/.cache", "", "home/user/.cache", true);
    test_pattern("/**/.cache", "", "home/user/.cache", true);
    test_pattern(".cache", "", "home/user/.cache", true);
    test_pattern("?.o", "", "home/user/a.o", true);
    test_pattern("?.o", "", "home/user/a/o", false);
    test_pattern("*.o", "", "a/b/\n.o", true);
    test_pattern("/**/.cache", "", "home/new\nline/.cache", true);
    test_pattern("/*/.cache", "home", "home/user/.cache", true);
    test_pattern(".cache", "home", "home/user/.cache", true);
    test_pattern("user/.cache", "home", "home/user/.cache", true);
    test_pattern("user/.cache", "home/user", "home/user/.cache", false);

    PathFilterList pfl;
    pfl.add_pattern(PathFilterList::DIRMERGE, ".cumulus-filter", "");
    pfl.save();
    pfl.merge_patterns("dir/.cumulus-filter", "dir",
                       "# comment\n"
                       "\n"
                       "- *.o\n"
                       "+ /.git/\n"
                       "* invalid\n");
    pfl.restore();
    return 0;
}
#endif
