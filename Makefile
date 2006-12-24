PACKAGES=uuid
CXXFLAGS=-O -Wall -D_FILE_OFFSET_BITS=64 -g -pg \
	 `pkg-config --cflags $(PACKAGES)`
LDFLAGS=-g -pg `pkg-config --libs $(PACKAGES)`

SRCS=scandir.cc sha1.cc store.cc
OBJS=$(SRCS:.cc=.o)

scandir : $(OBJS)
	$(CXX) $(LDFLAGS) -o $@ $^

dep:
	touch Makefile.dep
	makedepend -fMakefile.dep $(SRCS)

-include *.dep

