Cumulus File Selection (Includes/Excludes)
==========================================

A backup tool should support a flexible mechanism for selecting which
files should be included in or excluded from the backup.  This file
describes the mechanism used by Cumulus.  It is loosely based on the
mechanism used by rsync, though greatly simplified, and it allows for
users to specify include/exclude rules on a per-directory basis (if
enabled when running a backup).

Cumulus will back up, recursively, each of the files or directories
specified on the command-line, subject to include/exclude rules which
may cause some files to be skipped.  Each file is matched against the
list of rules in sequence.  The first rule which matches determines
whether the file should be included or excluded--thus, more specific
rules should be listed before more general rules.

There are four types of rules, modeled after those in rsync:

exclude (``-``)
    matches files which should not be backed up

include (``+``)
    matches files to be backed up (even if a later exclude rule would
    match)

dir-merge (``:``)
    specify a file which, if found in a directory during the backup
    process, will be read to insert additional rules for that directory
    and its subdirectories

merge (``.``)
    immediately read a file containing additional rules and insert those
    in the current ruleset **(not yet implemented)**

Patterns found in the rules are interpreted as follows:

- Most characters are treated literally and must match exactly.
- A ``*`` matches zero or more characters, but not ``/``.
- A ``**`` matches zero or more characters, including ``/``.
- A ``?`` matches any single character, except for ``/``.
- A pattern starting with a ``/`` is matched against the complete path
  to the file.  A pattern without a leading ``/`` can match any suffix
  of the directory components.
- A pattern ending in a ``/`` matches directories only.

Note that dotfiles are not considered specially by these rules: a
pattern of "*" matches all files in a directory, including those
starting with a '.'.  This is different from the handling of the shell.

Merged Patterns
---------------

If a file matching a dir-merge rule is encountered, that file is parsed
to yield additional filter rules; those filter rules are inserted
immediately after the dir-merge rule, then removed when leaving the
directory containing the dir-merge file.

Blank lines and lines beginning with ``#`` in the file are ignored.
Otherwise, lines should consist of a single character indicating the
rule type (``+``, ``-``, or ``:`` for include, exclude, or dir-merge), a
space, and a file pattern.

Any patterns added by a dir-merge rule are matched relative to the
directory with the patterns file: so, for example, a pattern
"``+ /file.txt``" would match ``file.txt`` in the same directory as the
merge file, not at the root.

Example
-------

Suppose that cumulus is launched with a single filter argument:
``--dir-merge=/.cumulus-root-filter``.

``/.cumulus-root-filter``::

    # Ignore pseudo-filesystems and temporary directories
    - /proc/
    - /sys/
    # Exclude anywhere directories named tmp (except /var/tmp).
    # Files named tmp will still be included.
    + /var/tmp/
    - tmp/
    # Merge any user-provided rules in a file named .cumulus-filter here
    : .cumulus-filter
    # Ignore backup files anywhere in the file system
    - *~
    - *.bak
    # Ignore per-user cache directories
    - /home/*/.cache/

``/home/user/.cumulus-filter``::

    # Ignore the /home/user/scratch directory
    - /scratch/
    # Ignore vim swap files (in /home/user only)
    - .*.swp
    # Do keep backup files (overrides the global exclude rule)
    + *~
    # This rule ineffective: the top-level tmp/ rule has a higher precedence
    + tmp/

``/home/user/workspace/.cumulus-filter``::

    # Exclude backup files again in /home/user/workspace: this has a
    # higher precedence than the rules in /home/user/.cumulus-filter
    - *~

