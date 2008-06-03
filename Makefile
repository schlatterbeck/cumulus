PACKAGES=sqlite3 uuid
DEBUG=-g #-pg
CXXFLAGS=-O -Wall -D_FILE_OFFSET_BITS=64 $(DEBUG) \
	 `pkg-config --cflags $(PACKAGES)` -DCUMULUS_VERSION=`cat version`
LDFLAGS=$(DEBUG) `pkg-config --libs $(PACKAGES)`

SRCS=chunk.cc localdb.cc metadata.cc ref.cc remote.cc scandir.cc sha1.cc \
     store.cc subfile.cc util.cc
OBJS=$(SRCS:.cc=.o)

cumulus : $(OBJS)
	$(CXX) $(LDFLAGS) -o $@ $^

version : NEWS
	(git-describe || (head -n1 NEWS | cut -d" " -f1)) >version 2>/dev/null
$(OBJS) : version

clean :
	rm -f $(OBJS) lbs version

dep :
	touch Makefile.dep
	makedepend -fMakefile.dep $(SRCS)

.PHONY : clean dep

-include *.dep
