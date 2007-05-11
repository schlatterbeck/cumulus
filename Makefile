PACKAGES=uuid
DEBUG=-g #-pg
CXXFLAGS=-O -Wall -D_FILE_OFFSET_BITS=64 $(DEBUG) \
	 `pkg-config --cflags $(PACKAGES)`
LDFLAGS=$(DEBUG) -ltar `pkg-config --libs $(PACKAGES)`

SRCS=format.cc scandir.cc sha1.cc store.cc
OBJS=$(SRCS:.cc=.o)

scandir : $(OBJS)
	$(CXX) $(LDFLAGS) -o $@ $^

dep:
	touch Makefile.dep
	makedepend -fMakefile.dep $(SRCS)

-include *.dep

