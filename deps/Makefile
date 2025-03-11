# Redis dependency Makefile

uname_S:= $(shell sh -c 'uname -s 2>/dev/null || echo not')

CCCOLOR="\033[34m"
LINKCOLOR="\033[34;1m"
SRCCOLOR="\033[33m"
BINCOLOR="\033[37;1m"
MAKECOLOR="\033[32;1m"
ENDCOLOR="\033[0m"

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
	-(cd xredis-gtid && [ -f Makefile ] && $(MAKE) clean) > /dev/null || true
	-(cd tdigest && $(MAKE) clean) > /dev/null || true
	-(rm -f .make-*)

.PHONY: distclean

ifeq ($(BUILD_TLS),yes)
    HIREDIS_MAKE_FLAGS = USE_SSL=1
endif

hiredis: .make-prerequisites
	@printf '%b %b\n' $(MAKECOLOR)MAKE$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR)
	cd hiredis && $(MAKE) static $(HIREDIS_MAKE_FLAGS)

.PHONY: hiredis

linenoise: .make-prerequisites
	@printf '%b %b\n' $(MAKECOLOR)MAKE$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR)
	cd linenoise && $(MAKE)

.PHONY: linenoise

hdr_histogram: .make-prerequisites
	@printf '%b %b\n' $(MAKECOLOR)MAKE$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR)
	cd hdr_histogram && $(MAKE)

.PHONY: hdr_histogram

TDIGEST_CFLAGS= -I./include -I../../src

tdigest: .make-prerequisites
	@printf '%b %b\n' $(MAKECOLOR)MAKE$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR)
	cd tdigest && $(MAKE) CFLAGS="$(TDIGEST_CFLAGS)"

.PHONY: tdigest

ifeq ($(uname_S),SunOS)
	# Make isinf() available
	LUA_CFLAGS= -D__C99FEATURES__=1
endif

LUA_CFLAGS+= -O2 -Wall -DLUA_ANSI -DENABLE_CJSON_GLOBAL -DREDIS_STATIC='' -DLUA_USE_MKSTEMP $(CFLAGS)
LUA_LDFLAGS+= $(LDFLAGS)
# lua's Makefile defines AR="ar rcu", which is unusual, and makes it more
# challenging to cross-compile lua (and redis).  These defines make it easier
# to fit redis into cross-compilation environments, which typically set AR.
AR=ar
ARFLAGS=rc

lua: .make-prerequisites
	@printf '%b %b\n' $(MAKECOLOR)MAKE$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR)
	cd lua/src && $(MAKE) all CFLAGS="$(LUA_CFLAGS)" MYLDFLAGS="$(LUA_LDFLAGS)"

.PHONY: lua

JEMALLOC_CFLAGS= -std=gnu99 -Wall -pipe -g3 -O3 -funroll-loops $(CFLAGS)
JEMALLOC_LDFLAGS= $(LDFLAGS)

jemalloc: .make-prerequisites
	@printf '%b %b\n' $(MAKECOLOR)MAKE$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR)
	cd jemalloc && ./configure --with-version=5.1.0-0-g0 --with-lg-quantum=3 --with-jemalloc-prefix=je_ CFLAGS="$(JEMALLOC_CFLAGS)" LDFLAGS="$(JEMALLOC_LDFLAGS)"
	cd jemalloc && $(MAKE) CFLAGS="$(JEMALLOC_CFLAGS)" LDFLAGS="$(JEMALLOC_LDFLAGS)" lib/libjemalloc.a

.PHONY: jemalloc

# -Wno-unused-but-set-variable: meaning do not report warning when meeting unused but set variable
# -Wno-error: meaning do not report error when meeting warnings
# In ubuntu 24.04, do not compile rocksdb with sanitizer, which will cause sanitizer internal crash.
CFLAGS += -Wno-unused-but-set-variable -Wno-error
ROCKSDB_CFLAGS = $(CFLAGS)
ROCKSDB_BUILD_FLAGS=ROCKSDB_DISABLE_BZIP=1 ROCKSDB_DISABLE_LZ4=1 ROCKSDB_DISABLE_ZSTD=1 ROCKSDB_DISABLE_MALLOC_USABLE_SIZE=1 ROCKSDB_DISABLE_MEMKIND=1 PORTABLE=1 USE_COROUTINES=1 USE_FOLLY=1 ROCKSDB_USE_IO_URING=1

./rocksdb/_build_folly/folly/lib/libfolly.a:
	cd rocksdb && $(ROCKSDB_BUILD_FLAGS) $(MAKE) checkout_folly
	cd rocksdb && $(ROCKSDB_BUILD_FLAGS) $(MAKE) build_folly
	mkdir -p ./rocksdb/_build_folly
	FBUILD_PATH=`ls -d /tmp/fbcode_builder_getdeps*`; \
	BOOST_PATH=`ls -d $${FBUILD_PATH}/installed/boost*`; \
	DBL_CONV_PATH=`ls -d $${FBUILD_PATH}/installed/double-conversion*`; \
	FMT_PATH=`ls -d $${FBUILD_PATH}/installed/fmt*`; \
	GLOG_PATH=`ls -d $${FBUILD_PATH}/installed/glog*`; \
	GFLAGS_PATH=`ls -d $${FBUILD_PATH}/installed/gflags*`; \
	LIBEVENT_PATH=`ls -d $${FBUILD_PATH}/installed/libevent*`; \
	LIBIBERTY_PATH=`ls -d $${FBUILD_PATH}/installed/libiberty*`; \
	GLOG_LIB_PATH=`ls -d $${GLOG_PATH}/lib*`; \
	FMT_LIB_PATH=`ls -d $${FMT_PATH}/lib*`; \
	LIBIBERTY_LIB_PATH=`if [ -f $${LIBIBERTY_PATH}/lib/libiberty.a ]; then echo "$${LIBIBERTY_PATH}/lib"; elif [ -f $${LIBIBERTY_PATH}/lib64/libiberty.a ]; then echo "$${LIBIBERTY_PATH}/lib64"; else echo ""; fi` ;\
	cp -r $${BOOST_PATH} ./rocksdb/_build_folly/boost; \
	cp -r $${DBL_CONV_PATH} ./rocksdb/_build_folly/double-conversion; \
	mkdir -p ./rocksdb/_build_folly/fmt; \
	cp -r $${FMT_PATH}/include ./rocksdb/_build_folly/fmt/include; \
	cp -r $${FMT_LIB_PATH} ./rocksdb/_build_folly/fmt/lib; \
	mkdir -p ./rocksdb/_build_folly/glog; \
	cp -r $${GLOG_PATH}/include ./rocksdb/_build_folly/glog/include; \
	cp -r $${GLOG_LIB_PATH} ./rocksdb/_build_folly/glog/lib; \
	mkdir -p ./rocksdb/_build_folly/libiberty; \
	cp -r $${LIBIBERTY_PATH}/include ./rocksdb/_build_folly/libiberty/include; \
	cp -r $${LIBIBERTY_LIB_PATH} ./rocksdb/_build_folly/libiberty/lib; \
	cp -r $${GFLAGS_PATH} ./rocksdb/_build_folly/gflags; \
	cp -r $${LIBEVENT_PATH} ./rocksdb/_build_folly/libevent; \
	cp -r $${FBUILD_PATH}/installed/folly ./rocksdb/_build_folly/folly



rocksdb: ./rocksdb/_build_folly/folly/lib/libfolly.a .make-prerequisites
	@printf '%b %b\n' $(MAKECOLOR)MAKE$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR)
	cd rocksdb && $(ROCKSDB_BUILD_FLAGS) $(MAKE) CFLAGS="$(ROCKSDB_CFLAGS)" static_lib

.PHONY: rocksdb

XREDIS_GTID_CFLAGS= -std=gnu99 -Wall -pipe -g3 -O3 -funroll-loops $(CFLAGS)
XREDIS_GTID_LDFLAGS= $(LDFLAGS)

ifndef SANITIZER
    XREDIS_GTID_CFLAGS+=-I../../src -DUSE_JEMALLOC -I../jemalloc/include -DGTID_MALLOC_INCLUDE=\<xredis_gtid_zmalloc.h\>
endif

xredis-gtid: jemalloc .make-prerequisites
	@printf '%b %b\n' $(MAKECOLOR)MAKE$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR)
	cd xredis-gtid && $(MAKE) CFLAGS="$(XREDIS_GTID_CFLAGS)" LDFLAGS="$(XREDIS_GTID_LDFLAGS)"

.PHONY: xredis-gtid
