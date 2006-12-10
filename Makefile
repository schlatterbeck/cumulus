CXXFLAGS=-O -Wall -D_FILE_OFFSET_BITS=64

OBJS=scandir.o

scandir : $(OBJS)
	$(CXX) $(LDFLAGS) -o $@ $^
