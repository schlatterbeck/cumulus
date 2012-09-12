PACKAGES=sqlite3 uuid
DEBUG=-g
CXXFLAGS=-O -Wall -Wextra -D_FILE_OFFSET_BITS=64 $(DEBUG) \
	 $(shell pkg-config --cflags $(PACKAGES)) \
	 -DCUMULUS_VERSION=$(shell cat version)
LDFLAGS=$(DEBUG) $(shell pkg-config --libs $(PACKAGES))

SRCS=chunk.cc exclude.cc localdb.cc main.cc metadata.cc ref.cc remote.cc \
     sha1.cc store.cc subfile.cc util.cc
OBJS=$(SRCS:.cc=.o)

cumulus : $(OBJS)
	$(CXX) -o $@ $^ $(LDFLAGS)

version : NEWS
	(git describe || (head -n1 NEWS | cut -d" " -f1)) >version 2>/dev/null
$(OBJS) : version

clean :
	rm -f $(OBJS) cumulus version

dep :
	touch Makefile.dep
	makedepend -fMakefile.dep $(SRCS)

.PHONY : clean dep

-include *.dep
