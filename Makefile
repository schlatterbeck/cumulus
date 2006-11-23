CXXFLAGS=-O -Wall

OBJS=scandir.o

scandir : $(OBJS)
	$(CXX) $(LDFLAGS) -o $@ $^
