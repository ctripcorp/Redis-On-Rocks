# ctrip_storage 静态库 Makefile
# 编译冷热分离存储引擎为 libctrip_storage.a，集成到 redis-server

STD= -std=gnu11
WARN= -Wall
OPT= -Os

# 关键：需要反向引用 src/ 目录获取 server.h 及其依赖的所有头文件
# 同时需要引用其他 deps 的头文件（server.h 会 include hiredis、lua 等）
REDIS_SRC_DIR= ../../src
DEPS_DIR= ..

R_CFLAGS= $(STD) $(WARN) $(OPT) $(DEBUG) $(CFLAGS) \
    -I. \
    -I$(REDIS_SRC_DIR) \
    -I$(DEPS_DIR)/hiredis \
    -I$(DEPS_DIR)/linenoise \
    -I$(DEPS_DIR)/lua/src \
    -I$(DEPS_DIR)/hdr_histogram \
    -I$(DEPS_DIR)/fpconv \
    -I$(DEPS_DIR)/fast_float \
    -I$(DEPS_DIR)/xxhash
R_LDFLAGS= $(LDFLAGS)
DEBUG= -g

R_CC=$(CC) $(R_CFLAGS)

AR= ar
ARFLAGS= rcs

# ============================================================
# 自动版本适配：根据 Redis 版本选择 versions/functions_X_Y[_Z].c
# ============================================================
# 从 version.h 提取三段版本号 (major.minor.patch)
REDIS_VERSION_MAJOR := $(shell grep 'define REDIS_VERSION ' $(REDIS_SRC_DIR)/version.h | sed 's/.*"\([0-9]*\)\..*/\1/')
REDIS_VERSION_MINOR := $(shell grep 'define REDIS_VERSION ' $(REDIS_SRC_DIR)/version.h | sed 's/.*"[0-9]*\.\([0-9]*\)\..*/\1/')
REDIS_VERSION_PATCH := $(shell grep 'define REDIS_VERSION ' $(REDIS_SRC_DIR)/version.h | sed 's/.*"[0-9]*\.[0-9]*\.\([0-9]*\)".*/\1/')

# 版本数值化: major * 1000000 + minor * 1000 + patch
# 例如 8.6.2 → 8006002，每段最大支持 999
REDIS_VER_NUM := $(shell echo $$(( $(REDIS_VERSION_MAJOR) * 1000000 + $(REDIS_VERSION_MINOR) * 1000 + $(REDIS_VERSION_PATCH) )))

# 扫描 versions/functions_*.c，选择版本号 <= 当前 Redis 版本中最接近的
# 支持三种命名: functions_8_6_2.c (8.6.2) / functions_8_6.c (8.6.0) / functions_8.c (8.0.0)
VERSIONS_FUNC_SRC := $(shell \
    best=""; best_num=0; \
    for f in versions/functions_*.c; do \
        [ -f "$$f" ] || continue; \
        ver=$$(basename "$$f" .c | sed 's/^functions_//'); \
        major=$$(echo "$$ver" | awk -F_ '{print $$1}'); \
        minor=$$(echo "$$ver" | awk -F_ '{print (NF>=2 ? $$2 : 0)}'); \
        patch=$$(echo "$$ver" | awk -F_ '{print (NF>=3 ? $$3 : 0)}'); \
        num=$$(( major * 1000000 + minor * 1000 + patch )); \
        if [ $$num -le $(REDIS_VER_NUM) ] && [ $$num -gt $$best_num ]; then \
            best="$$f"; best_num=$$num; \
        fi; \
    done; \
    echo "$$best")

VERSIONS_FUNC_OBJ := $(VERSIONS_FUNC_SRC:.c=.o)

# 未找到匹配版本时编译报错
ifeq ($(VERSIONS_FUNC_SRC),)
$(error 没有找到与 Redis $(REDIS_VERSION_MAJOR).$(REDIS_VERSION_MINOR).$(REDIS_VERSION_PATCH) 匹配的版本适配文件，请在 versions/ 下添加对应的 functions_X_Y.c)
endif

# 所有源文件对应的 .o
OBJS= ctrip_storage.o \
      ctrip_storage_client.o \
      ctrip_storage_rate_limit.o \
      ctrip_storage_trace.o \
      ctrip_storage_lock.o \
      ctrip_storage_metric.o \
      ctrip_storage_object_meta.o \
      ctrip_storage_adlist.o \
      ctrip_storage_thread.o \
      ctrip_storage_data.o \
      ctrip_storage_rio.o \
      ctrip_storage_request_utils.o \
      ctrip_storage_request_meta_scan.o \
      ctrip_storage_request_async.o \
      ctrip_storage_request_sync.o \
      ctrip_storage_request_callback.o \
      ctrip_storage_thread_request.o \
      ctrip_storage_request.o \
      ctrip_storage_repl.o \
      ctrip_storage_object_meta.o \
      ctrip_storage_object_bitmap.o \
      ctrip_storage_object_list.o \
      ctrip_storage_object_set.o \
      ctrip_storage_object_zset.o \
      ctrip_storage_object_hash.o \
      ctrip_storage_object_wholekey.o \
      ctrip_storage_objects.o \
      ctrip_storage_commands.o \
      ctrip_storage_expire.o \
      memory_storage_engine.o \
      rocksdb_storage_engine.o \
      cuckoo_filter.o \
      ctrip_storage_filter.o \
      roaring_bitmap.o \
      absent_cache.o \
      buffered_allocator.o \
      $(VERSIONS_FUNC_OBJ)

libctrip_storage.a: $(OBJS)
	$(AR) $(ARFLAGS) $@ $^

# versions 子目录编译规则（自动适配版本）
$(VERSIONS_FUNC_OBJ): $(VERSIONS_FUNC_SRC) versions/functions.h
	@mkdir -p versions
	@echo "版本适配: Redis $(REDIS_VERSION_MAJOR).$(REDIS_VERSION_MINOR).$(REDIS_VERSION_PATCH) → $(VERSIONS_FUNC_SRC)"
	$(R_CC) -c $< -o $@

# 从 commands/*.json 生成 swap命令定义
ctrip_storage_commands.c: commands/*.json generate_swap_command_def.py
	python3 generate_swap_command_def.py

.c.o:
	$(R_CC) -c $<

clean:
	rm -f *.o versions/*.o
	rm -f *.a
