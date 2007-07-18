PACKAGES=sqlite3 uuid
DEBUG=-g #-pg
CXXFLAGS=-O -Wall -D_FILE_OFFSET_BITS=64 $(DEBUG) \
	 `pkg-config --cflags $(PACKAGES)` -DLBS_VERSION=`cat version`
LDFLAGS=$(DEBUG) -ltar `pkg-config --libs $(PACKAGES)`

SRCS=localdb.cc ref.cc scandir.cc sha1.cc statcache.cc store.cc util.cc
OBJS=$(SRCS:.cc=.o)

lbs : $(OBJS)
	$(CXX) $(LDFLAGS) -o $@ $^

version :
	(git-describe || echo "Unknown") >version
$(OBJS) : version

dep:
	touch Makefile.dep
	makedepend -fMakefile.dep $(SRCS)

-include *.dep

