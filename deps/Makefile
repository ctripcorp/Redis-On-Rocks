# Redis dependency Makefile

uname_S:= $(shell sh -c 'uname -s 2>/dev/null || echo not')

LUA_DEBUG?=no
LUA_COVERAGE?=no

CCCOLOR="\033[34m"
LINKCOLOR="\033[34;1m"
SRCCOLOR="\033[33m"
BINCOLOR="\033[37;1m"
MAKECOLOR="\033[32;1m"
ENDCOLOR="\033[0m"

DEPS_CFLAGS := $(CFLAGS)
DEPS_LDFLAGS := $(LDFLAGS)
CLANG := $(findstring clang,$(shell sh -c '$(CC) --version | head -1'))

# MSan looks for errors related to uninitialized memory.
# Make sure to build the dependencies with MSan as it needs all the code to be instrumented.
# A library could be used to initialize memory but if it's not build with --fsanitize=memory then
# MSan doesn't know about it and will spit false positive error when that memory is then used.
ifeq ($(SANITIZER),memory)
ifeq (clang, $(CLANG))
	DEPS_CFLAGS+=-fsanitize=memory -fsanitize-memory-track-origins=2 -fno-sanitize-recover=all -fno-omit-frame-pointer
	DEPS_LDFLAGS+=-fsanitize=memory
else
    $(error "MemorySanitizer needs to be compiled and linked with clang. Please use CC=clang")
endif
endif

default:
	@echo "Explicit target required"

.PHONY: default

# Prerequisites target
.make-prerequisites:
	@touch $@

# Clean everything when CFLAGS is different
ifneq ($(shell sh -c '[ -f .make-cflags ] && cat .make-cflags || echo none'), $(CFLAGS))
.make-cflags: distclean
	-(echo "$(CFLAGS)" > .make-cflags)
.make-prerequisites: .make-cflags
endif

# Clean everything when LDFLAGS is different
ifneq ($(shell sh -c '[ -f .make-ldflags ] && cat .make-ldflags || echo none'), $(LDFLAGS))
.make-ldflags: distclean
	-(echo "$(LDFLAGS)" > .make-ldflags)
.make-prerequisites: .make-ldflags
endif

distclean:
	-(cd hiredis && $(MAKE) clean) > /dev/null || true
	-(cd linenoise && $(MAKE) clean) > /dev/null || true
	-(cd lua && $(MAKE) clean) > /dev/null || true
	-(cd jemalloc && [ -f Makefile ] && $(MAKE) distclean) > /dev/null || true
	-(cd hdr_histogram && $(MAKE) clean) > /dev/null || true
	-(cd fpconv && $(MAKE) clean) > /dev/null || true
	-(cd fast_float && $(MAKE) clean) > /dev/null || true
	-(cd xredis-gtid && [ -f Makefile ] && $(MAKE) clean) > /dev/null || true
	-(rm -f .make-*)

.PHONY: distclean

ifneq (,$(filter $(BUILD_TLS),yes module))
    HIREDIS_MAKE_FLAGS = USE_SSL=1
endif

# Special care is needed to pass additional CFLAGS/LDFLAGS to hiredis as it
# modifies these variables in its Makefile.
hiredis: .make-prerequisites
	@printf '%b %b\n' $(MAKECOLOR)MAKE$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR)
	cd hiredis && $(MAKE) static $(HIREDIS_MAKE_FLAGS) HIREDIS_CFLAGS="$(DEPS_CFLAGS)" HIREDIS_LDFLAGS="$(DEPS_LDFLAGS)"

.PHONY: hiredis

linenoise: .make-prerequisites
	@printf '%b %b\n' $(MAKECOLOR)MAKE$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR)
	cd linenoise && $(MAKE) CFLAGS="$(DEPS_CFLAGS)" LDFLAGS="$(DEPS_LDFLAGS)"

.PHONY: linenoise

hdr_histogram: .make-prerequisites
	@printf '%b %b\n' $(MAKECOLOR)MAKE$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR)
	cd hdr_histogram && $(MAKE) CFLAGS="$(DEPS_CFLAGS)" LDFLAGS="$(DEPS_LDFLAGS)"

.PHONY: hdr_histogram

fpconv: .make-prerequisites
	@printf '%b %b\n' $(MAKECOLOR)MAKE$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR)
	cd fpconv && $(MAKE) CFLAGS="$(DEPS_CFLAGS)" LDFLAGS="$(DEPS_LDFLAGS)"

.PHONY: fpconv

fast_float: .make-prerequisites
	@printf '%b %b\n' $(MAKECOLOR)MAKE$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR)
	cd fast_float && $(MAKE) libfast_float CFLAGS="$(DEPS_CFLAGS)" LDFLAGS="$(DEPS_LDFLAGS)"

.PHONY: fast_float

ifeq ($(uname_S),SunOS)
	# Make isinf() available
	LUA_CFLAGS= -D__C99FEATURES__=1
endif

LUA_CFLAGS+= -Wall -DLUA_ANSI -DENABLE_CJSON_GLOBAL -DREDIS_STATIC='' -DLUA_USE_MKSTEMP $(DEPS_CFLAGS)
LUA_LDFLAGS+= $(DEPS_LDFLAGS)
ifeq ($(LUA_DEBUG),yes)
	LUA_CFLAGS+= -O0 -g -DLUA_USE_APICHECK
else
	LUA_CFLAGS+= -O2 
endif
ifeq ($(LUA_COVERAGE),yes)
	LUA_CFLAGS += -fprofile-arcs -ftest-coverage
	LUA_LDFLAGS += -fprofile-arcs -ftest-coverage
endif

# lua's Makefile defines AR="ar rcu", which is unusual, and makes it more
# challenging to cross-compile lua (and redis).  These defines make it easier
# to fit redis into cross-compilation environments, which typically set AR.
AR=ar
ARFLAGS=rc

lua: .make-prerequisites
	@printf '%b %b\n' $(MAKECOLOR)MAKE$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR)
	cd lua/src && $(MAKE) all CFLAGS="$(LUA_CFLAGS)" MYLDFLAGS="$(LUA_LDFLAGS)" AR="$(AR) $(ARFLAGS)"

.PHONY: lua

JEMALLOC_CFLAGS=$(CFLAGS)
JEMALLOC_LDFLAGS=$(LDFLAGS)

ifneq ($(DEB_HOST_GNU_TYPE),)
JEMALLOC_CONFIGURE_OPTS += --host=$(DEB_HOST_GNU_TYPE)
endif

jemalloc: .make-prerequisites
	@printf '%b %b\n' $(MAKECOLOR)MAKE$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR)
	cd jemalloc && ./configure --disable-cxx --with-version=5.3.0-0-g0 --with-lg-quantum=3 --disable-cache-oblivious --with-jemalloc-prefix=je_ CFLAGS="$(JEMALLOC_CFLAGS)" LDFLAGS="$(JEMALLOC_LDFLAGS)" $(JEMALLOC_CONFIGURE_OPTS)
	cd jemalloc && $(MAKE) lib/libjemalloc.a
.PHONY: jemalloc

# -Wno-unused-but-set-variable: meaning do not report warning when meeting unused but set variable
# -Wno-error: meaning do not report error when meeting warnings
# In ubuntu 24.04, do not compile rocksdb with sanitizer, which will cause sanitizer internal crash.
CFLAGS += -Wno-unused-but-set-variable -Wno-error
ROCKSDB_CFLAGS = $(CFLAGS)
ROCKSDB_BUILD_FOLLY_FLAGS=ROCKSDB_DISABLE_BZIP=1 ROCKSDB_DISABLE_LZ4=1 ROCKSDB_DISABLE_ZSTD=1 ROCKSDB_DISABLE_MALLOC_USABLE_SIZE=1 ROCKSDB_DISABLE_MEMKIND=1 PORTABLE=1  ROCKSDB_USE_IO_URING=1
ROCKSDB_BUILD_FLAGS=USE_COROUTINES=1 USE_FOLLY=1 $(ROCKSDB_BUILD_FOLLY_FLAGS) 

./rocksdb/_build_folly/folly/lib/libfolly.a:
	if [ ! -f rocksdb/_build_folly/installed/folly/lib/libfolly.a ]; then \
		cd rocksdb && ${ROCKSDB_BUILD_FOLLY_FLAGS} ${MAKE} checkout_folly && ${ROCKSDB_BUILD_FOLLY_FLAGS} ${MAKE} build_folly; \
	fi
	rm -rf ./rocksdb/_build_folly/libs
	mkdir -p ./rocksdb/_build_folly/libs
	FBUILD_PATH=`ls -d ./rocksdb/_build_folly*`; \
	BOOST_PATH=`ls -d $${FBUILD_PATH}/installed/boost*`; \
	DBL_CONV_PATH=`ls -d $${FBUILD_PATH}/installed/double-conversion*`; \
	FMT_PATH=`ls -d $${FBUILD_PATH}/installed/fmt*`; \
	GLOG_PATH=`ls -d $${FBUILD_PATH}/installed/glog*`; \
	GFLAGS_PATH=`ls -d $${FBUILD_PATH}/installed/gflags*`; \
	LIBEVENT_PATH=`ls -d $${FBUILD_PATH}/installed/libevent*`; \
	GLOG_LIB_PATH=`ls -d $${GLOG_PATH}/lib*`; \
	FMT_LIB_PATH=`ls -d $${FMT_PATH}/lib*`; \
	cp -r $${BOOST_PATH} ./rocksdb/_build_folly/libs/boost; \
	cp -r $${DBL_CONV_PATH} ./rocksdb/_build_folly/libs/double-conversion; \
	mkdir -p ./rocksdb/_build_folly/libs/fmt; \
	cp -r $${FMT_PATH}/include ./rocksdb/_build_folly/libs/fmt/include; \
	cp -r $${FMT_LIB_PATH} ./rocksdb/_build_folly/libs/fmt/lib; \
	mkdir -p ./rocksdb/_build_folly/libs/glog; \
	cp -r $${GLOG_PATH}/include ./rocksdb/_build_folly/libs/glog/include; \
	cp -r $${GLOG_LIB_PATH} ./rocksdb/_build_folly/libs/glog/lib; \
	mkdir -p ./rocksdb/_build_folly/libs/libiberty; \
	cp -r $${GFLAGS_PATH} ./rocksdb/_build_folly/libs/gflags; \
	cp -r $${LIBEVENT_PATH} ./rocksdb/_build_folly/libs/libevent; \
	cp -r $${FBUILD_PATH}/installed/folly ./rocksdb/_build_folly/libs/folly



rocksdb: ./rocksdb/_build_folly/folly/lib/libfolly.a .make-prerequisites
	@printf '%b %b\n' $(MAKECOLOR)MAKE$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR)
	cd rocksdb && $(ROCKSDB_BUILD_FLAGS) $(MAKE) CFLAGS="$(ROCKSDB_CFLAGS)" static_lib

.PHONY: rocksdb

XREDIS_GTID_CFLAGS= -std=gnu99 -Wall -pipe -g3 -O3 -funroll-loops $(CFLAGS)
XREDIS_GTID_LDFLAGS= $(LDFLAGS)

ifndef SANITIZER
    XREDIS_GTID_CFLAGS+=-I../../src -DUSE_JEMALLOC -I../jemalloc/include -DGTID_MALLOC_INCLUDE=\<xredis_gtid_zmalloc.h\>
	XREDIS_GTID_DEPS=jemalloc	
endif

xredis-gtid:$(XREDIS_GTID_DEPS) .make-prerequisites
	@printf '%b %b\n' $(MAKECOLOR)MAKE$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR)
	cd xredis-gtid && $(MAKE) CFLAGS="$(XREDIS_GTID_CFLAGS)" LDFLAGS="$(XREDIS_GTID_LDFLAGS)"

.PHONY: xredis-gtid


