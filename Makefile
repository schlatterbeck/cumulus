CXXFLAGS=-O -Wall -D_FILE_OFFSET_BITS=64 -g
LDFLAGS=-g

OBJS=scandir.o sha1.o store.o

scandir : $(OBJS)
	$(CXX) $(LDFLAGS) -o $@ $^
