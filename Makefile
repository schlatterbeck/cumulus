PACKAGES=sqlite3 uuid
DEBUG=-g #-pg
CXXFLAGS=-O -Wall -D_FILE_OFFSET_BITS=64 $(DEBUG) \
	 `pkg-config --cflags $(PACKAGES)` -DLBS_VERSION=`cat version`
LDFLAGS=$(DEBUG) `pkg-config --libs $(PACKAGES)`

SRCS=localdb.cc ref.cc scandir.cc sha1.cc statcache.cc store.cc util.cc
OBJS=$(SRCS:.cc=.o)

lbs : $(OBJS)
	$(CXX) $(LDFLAGS) -o $@ $^

version : NEWS
	(git-describe || (head -n1 NEWS | cut -d" " -f1)) >version
$(OBJS) : version

clean :
	rm -f $(OBJS) lbs version

dep :
	touch Makefile.dep
	makedepend -fMakefile.dep $(SRCS)

.PHONY : clean dep

-include *.dep
