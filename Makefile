PACKAGES=sqlite3 uuid
DEBUG=-g
CXXFLAGS=-O -Wall -Wextra -D_FILE_OFFSET_BITS=64 $(DEBUG) \
	 $(shell pkg-config --cflags $(PACKAGES)) \
	 -DCUMULUS_VERSION=$(shell cat version)
LDFLAGS=$(DEBUG) $(shell pkg-config --libs $(PACKAGES))

THIRD_PARTY_SRCS=chunk.cc sha1.cc sha256.cc
SRCS=exclude.cc hash.cc localdb.cc main.cc metadata.cc ref.cc remote.cc \
     store.cc subfile.cc util.cc $(addprefix third_party/,$(THIRD_PARTY_SRCS))
OBJS=$(SRCS:.cc=.o)

all : cumulus cumulus-chunker-standalone

cumulus : $(OBJS)
	$(CXX) -o $@ $^ $(LDFLAGS)

cumulus-chunker-standalone : chunker-standalone.o third_party/chunk.o
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
