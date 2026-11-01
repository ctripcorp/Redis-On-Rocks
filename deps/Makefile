# Dependency Makefile

uname_S:= $(shell sh -c 'uname -s 2>/dev/null || echo not')

LUA_DEBUG?=no
LUA_COVERAGE?=no

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
	-(cd libvalkey && $(MAKE) clean) > /dev/null || true
	-(cd linenoise && $(MAKE) clean) > /dev/null || true
	-(cd lua && $(MAKE) clean) > /dev/null || true
	-(cd jemalloc && [ -f Makefile ] && $(MAKE) distclean) > /dev/null || true
	-(cd hdr_histogram && $(MAKE) clean) > /dev/null || true
	-(cd fpconv && $(MAKE) clean) > /dev/null || true
	-(cd lz4 && $(MAKE) clean) > /dev/null || true
	-(cd tdigest && $(MAKE) clean) > /dev/null || true
	-(cd xredis-gtid && $(MAKE) clean) > /dev/null || true
	-(rm -f .make-*)

distclean-rocksdb:
	-(cd rocksdb && $(MAKE) clean && rm -rf _build_folly* librocksdb.a) > /dev/null || true

.PHONY: distclean distclean-rocksdb

TDIGEST_CFLAGS= -I./include -I../../src

tdigest: .make-prerequisites
	@printf '%b %b\n' $(MAKECOLOR)MAKE$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR)
	cd tdigest && $(MAKE) CFLAGS="$(TDIGEST_CFLAGS)"

.PHONY: tdigest

LIBVALKEY_MAKE_FLAGS = SDS_INCLUDE_DIR=../../src/
ifneq (,$(filter $(BUILD_TLS),yes module))
    LIBVALKEY_MAKE_FLAGS += USE_TLS=1
endif

ifneq (,$(filter $(BUILD_RDMA),yes module))
    LIBVALKEY_MAKE_FLAGS += USE_RDMA=1
	ifneq (,$(filter $(BUILD_RDMA),module))
		LIBVALKEY_MAKE_FLAGS += USE_DLOPEN_RDMA=1
	endif
endif

libvalkey: .make-prerequisites
	@printf '%b %b\n' $(MAKECOLOR)MAKE$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR)
	cd libvalkey && $(MAKE) static $(LIBVALKEY_MAKE_FLAGS)

.PHONY: libvalkey

linenoise: .make-prerequisites
	@printf '%b %b\n' $(MAKECOLOR)MAKE$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR)
	cd linenoise && $(MAKE)

.PHONY: linenoise

hdr_histogram: .make-prerequisites
	@printf '%b %b\n' $(MAKECOLOR)MAKE$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR)
	cd hdr_histogram && $(MAKE)

.PHONY: hdr_histogram

fpconv: .make-prerequisites
	@printf '%b %b\n' $(MAKECOLOR)MAKE$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR)
	cd fpconv && $(MAKE) CFLAGS="-fPIC $(CFLAGS)"

.PHONY: fpconv

ifeq ($(uname_S),SunOS)
	# Make isinf() available
	LUA_CFLAGS= -D__C99FEATURES__=1
endif

LUA_CFLAGS+= -Wall -DLUA_ANSI -DENABLE_CJSON_GLOBAL -DLUA_USE_MKSTEMP $(CFLAGS) -fPIC
LUA_LDFLAGS+= $(LDFLAGS)
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

gtest-parallel: .make-prerequisites
	@printf '%b %b\n' $(MAKECOLOR)MAKE$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR)
	@if [ ! -f gtest-parallel/gtest_parallel.py ]; then \
		echo "Downloading gtest-parallel..."; \
		rm -rf gtest-parallel; \
		git clone --depth 1 https://github.com/google/gtest-parallel.git gtest-parallel; \
	fi

lz4: .make-prerequisites
	@printf '%b %b\n' $(MAKECOLOR)MAKE$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR)
	cd lz4 && $(MAKE) CFLAGS="$(CFLAGS)"

.PHONY: lz4

ROCKSDB_CFLAGS = $(CFLAGS) -Wno-unused-but-set-variable -Wno-error
ROCKSDB_COMMON_FLAGS = ROCKSDB_DISABLE_BZIP=1 ROCKSDB_DISABLE_LZ4=1 ROCKSDB_DISABLE_ZSTD=1 ROCKSDB_DISABLE_SNAPPY=1 ROCKSDB_DISABLE_MALLOC_USABLE_SIZE=1 ROCKSDB_DISABLE_MEMKIND=1 PORTABLE=1

ifeq ($(uname_S),Darwin)
rocksdb: .make-prerequisites
	@printf '%b %b\n' $(MAKECOLOR)MAKE$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR)
	cd rocksdb && $(ROCKSDB_COMMON_FLAGS) $(MAKE) CFLAGS="$(ROCKSDB_CFLAGS)" static_lib
else
ROCKSDB_BUILD_FOLLY_FLAGS=$(ROCKSDB_COMMON_FLAGS) ROCKSDB_USE_IO_URING=1
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
endif

.PHONY: rocksdb

XREDIS_GTID_CFLAGS= -std=gnu99 -Wall -pipe -g3 -O3 -funroll-loops $(CFLAGS)
XREDIS_GTID_LDFLAGS= $(LDFLAGS)

ifndef SANITIZER
ifeq ($(MALLOC),jemalloc)
    XREDIS_GTID_CFLAGS+=-I../../src -DUSE_JEMALLOC -I../jemalloc/include -DGTID_MALLOC_INCLUDE=\<xredis_gtid_zmalloc.h\>
endif
endif

xredis-gtid: .make-prerequisites
	@printf '%b %b\n' $(MAKECOLOR)MAKE$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR)
	cd xredis-gtid && $(MAKE) CFLAGS="$(XREDIS_GTID_CFLAGS)" LDFLAGS="$(XREDIS_GTID_LDFLAGS)"

.PHONY: xredis-gtid
