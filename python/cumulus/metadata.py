"""Advanced metadata iterator for Cumulus snapshots.

Allow fast random access to Cumulus metadata logs.  This requires that the
underlying metadata log have been generated by a depth-first-search traversal
of the filesystem, in sorted order.
"""

import cumulus

class Metadata:
    def __init__(self, object_store, root):
        self.store = object_store
        self.root = root

    def _load(self, ref):
        """Return the contents of the given object, as a list of text lines."""

        data = self.store.get(ref)
        return data.splitlines()

    def _read(self, ptr):
        """Parse the metadata item at the given address and return it."""

        if ptr is None or ptr == []: return {}

        (ref, n) = ptr[-1]
        lines = self._load(ref)[n:]

        try:
            return cumulus.parse(lines, lambda l: len(l) == 0).next()
        except StopIteration:
            return {}

    def _advance(self, ptr):
        """Advance the specified metadata pointer to the next metadata item."""

        if ptr == None:
            return None

        advanced = False
        ptr = list(ptr)
        if ptr == []:
            ptr.append((self.root, 0))
            advanced = True

        while True:
            last = ptr.pop(-1)
            lines = self._load(last[0])

            # Reached the end of the current object?  Advance pointer by one
            # line in the containing object (or return None if reached the very
            # end of input).
            if last[1] >= len(lines):
                if ptr == []: return None
                last = ptr.pop(-1)
                ptr.append((last[0], last[1] + 1))
                advanced = True
                continue

            # Reached a line with an indirect reference?  Recurse.
            line = lines[last[1]]
            if line.startswith('@'):
                ptr.append(last)
                ptr.append((line[1:], 0))
                advanced = True
                continue

            # Skip over blank lines.
            if line == "":
                ptr.append((last[0], last[1] + 1))
                advanced = True
                continue

            # Skip over the remainder of a metadata stanza, if we started in
            # the middle of one.
            if not advanced:
                ptr.append((last[0], last[1] + 1))
                continue

            # Done!  Found a non-blank line which is not an indirect reference.
            ptr.append(last)
            return ptr

    def _cmp(self, ptr1, ptr2):
        if ptr1 is None and ptr2 is None: return 0
        if ptr1 is None: return 1
        if ptr2 is None: return -1
        return cmp(ptr1, ptr2)

    def _get_path(self, metadata):
        if metadata is None or 'name' not in metadata:
            return None
        path = metadata['name']
        path = cumulus.MetadataItem.decode_str(path)
        if path == '.': return []
        return path.split('/')

    def _midpoint(self, ptr1, ptr2):
        """Return a pointer to some metadata item in the range [ptr1, ptr2)."""

        if ptr1 == []: ptr1 = self._advance([])
        if ptr1 is None: return None
        if self._cmp(ptr1, ptr2) > 0: return None

        #print "ptr1:", ptr1
        #print "ptr2:", ptr2

        level = 0
        while True:
            if level >= len(ptr1): return ptr1

            if ptr2 is not None and level < len(ptr2) and ptr1[level] == ptr2[level]:
                level += 1
                continue

            midpoint = ptr1[0:level]
            lastref = ptr1[level][0]
            limit = len(self._load(lastref))
            if ptr2 is not None and level < len(ptr2) \
                    and ptr1[level][0] == ptr2[level][0]:
                limit = ptr2[level][1]
            midpoint.append((lastref, (ptr1[level][1] + limit) // 2))

            if self._cmp(midpoint, ptr1) < 0:
                #print "    ...descend"
                level += 1
                continue

            #print "Guess midpoint:", midpoint
            midpoint = self._advance(midpoint)
            #print "    ...advanced to", midpoint

            if self._cmp(midpoint, ptr2) >= 0:
                #print "    ...overshoot, trying again"
                level += 1
                continue

            return midpoint

    def search(self, searchfunc, ptr1=[], ptr2=None):
        """Perform a binary search for name.

        The search is restricted to the interval [ptr1, ptr2).  Return either a
        pointer to the item for name, or if it doesn't exist the pointer to the
        item which follows where it would have been (assuming the original
        search interval included that location)."""

        def _printable(ptr):
            if ptr is None: return None
            return tuple(x[1] for x in ptr)

        while True:
            #print _printable(ptr1), "...", _printable(ptr2)
            if self._cmp(ptr1, ptr2) >= 0:
                #print "    X", _printable(ptr1)
                return ptr1

            mid = self._midpoint(ptr1, ptr2)
            midpath = self._get_path(self._read(mid))

            c = searchfunc(midpath)
            if c == 0:
                #print "    =", _printable(mid), midpath
                return mid
            elif c < 0:
                #print "    >", _printable(mid), midpath
                ptr1 = self._advance(mid)
            else:
                #print "    <", _printable(mid), midpath
                ptr2 = mid

if __name__ == '__main__':
    import cumulus

    store = cumulus.ObjectStore(cumulus.LowlevelDataStore('/backups/lbs/turin'))
    root = '9e0be287-a74c-4527-aa26-0a10e8f40f7d/00000243(sha1=316e034bb5b86e5ba00b1d54335427d7b742f4f9)'

    metadata = Metadata(store, root)
    ptr = metadata.search(['home', 'mvrable', 'docs'])
    print ptr
    print metadata._read(ptr)
    store.cleanup()