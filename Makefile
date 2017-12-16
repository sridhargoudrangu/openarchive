LIBSN   = openarchive
PACKAGE = lib$(LIBSN)
LIBEXT  = .so  

PWD         := $(shell pwd)
INC         := -I /usr/local/include -I include -I /usr/src/glusterfs/api/src\
               -I /usr/src/glusterfs/xlators/features/archive/src\
               -I /usr/include/glusterfs/api -I $(PWD)/cli/include

LIBS_PATH   := -L /usr/local/lib/ -L $(PWD)
LIBS        := -lboost_system -lboost_thread -lpthread -lboost_filesystem -ldl\
               -lboost_program_options -lboost_log -lboost_log_setup -luuid

C           ?= gcc 
CFLAGS      += -g $(INC)
CXX         ?= g++ 
CXXFLAGS    += -g -fPIC $(INC) -Wall -Werror -DVERSION=\"$(VERSION)\" \
               -std=c++11 -DBOOST_LOG_DYN_LINK -DUNIX
CXXDFLAGS   += -g -Wall -Werror -std=c++11

LDFLAGS     += $(LIBS_PATH) 
prefix      ?= /usr/local

headers        = $(wildcard include/*.h)
lib_hdr        = $(wildcard src/*.h)
lib_src        = $(wildcard src/*.cpp)
cli_src        = $(wildcard cli/*.c)
cli_headers    = $(wildcard cli/include/*.h)

lib_libs       = $(LIBS) 
extra_dist     = Makefile README.md
dist_files     = $(headers) $(lib_hdr) $(lib_src) 

.PHONY: all clean dist install default
default:all

CLI    = cli/openarchive
all: $(PACKAGE)$(LIBEXT) $(CLI)

$(PACKAGE)$(LIBEXT): $(patsubst %.cpp, %.o, $(lib_src)) Makefile
	$(CXX) -shared $(CXXFLAGS) $(LDFLAGS) $(filter-out Makefile,$^)\
        $(lib_libs) -o $@

%.o : %.cpp Makefile
	$(CXX) $(CXXFLAGS) -MD -c $< -o $(patsubst %.cpp, %.o, $<)

$(CLI): $(cli_src) Makefile
	$(C) $(CFLAGS) -MD $(filter-out Makefile,$^) -ldl -lpthread -o $@

clean: 
	rm -f src/*.o src/*.d cli/*.d
	rm -f $(PACKAGE)$(LIBEXT)
	rm -f $(CLI)

install:
	mkdir -p $(DESTDIR)/usr/local/lib
	mkdir -p $(DESTDIR)/usr/local/bin
	mkdir -p $(DESTDIR)/etc/ld.so.conf.d
	mkdir -p $(DESTDIR)/usr/include/openarchive
	cp -f $(PWD)/include/* $(DESTDIR)/usr/include/openarchive
	install -m 0755 $(PACKAGE)$(LIBEXT) $(DESTDIR)/usr/local/lib/
	install -m 0755 $(CLI) $(DESTDIR)/usr/local/bin/
	echo "/usr/local/lib" > $(DESTDIR)/etc/ld.so.conf.d/archivestore.conf

ifneq "$(MAKECMDGOALS)" "clean"
deps  = $(patsubst %.cpp, %.d, $(lib_src))
deps += $(patsubst %.c, %d, $(cli_src)) 
-include $(deps)
endif
