LIB=libedu_cornell_cs_blog_JNIBlog.so
SRCS=JNIBlog.cc InfiniBandRDMA.cc
HEADERS=JNIBlog.h map.h types.h InfiniBandRDMA.h bitmap.h
CC=g++ -O3 -std=c++14
CPPFLAGS=$(DFLAGS) -I$(JAVA_HOME)/include/ -I$(JAVA_HOME)/include/linux -Wall -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -DFLUSH_BATCHING -DDEBUG
LDFLAGS=-shared -fpic -pthread -libverbs -lrdmacm
TESTLDFLAGS=-libverbs -pthread -lrdmacm
TESTCPPFLAGS=-I$(JAVA_HOME)/include/ -I$(JAVA_HOME)/include/linux -Wall -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -DFLUSH_BATCHING -DDEBUG
TESTSRC=InfiniBandRDMA.cc test.cc
#TESTSRC=InfiniBandRDMA.c test.c
TESTPROG=test

all: $(LIB)

tests: $(TESTPROG)

$(LIB): $(SRCS) $(HEADERS)
	$(CC) $(CPPFLAGS) $(LDFLAGS) $(SRCS) -o $@

$(TESTPROG): $(TESTSRC)
	$(CC) $(TESTCPPFLAGS) $^ $(TESTLDFLAGS) -o $@
#	gcc $(TESTCPPFLAGS) $^ $(TESTLDFLAGS) -o $@
	
clean:
	rm -rf $(LIB) $(TESTPROG)
