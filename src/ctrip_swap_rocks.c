/* Copyright (c) 2021, ctrip.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"
#include <rocksdb/c.h>
#include <sys/statvfs.h>
#include <stddef.h>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

#define KB 1024
#define MB (1024*1024)

int rmdirRecursive(const char *path);
int rocksInit() {
    rocks *rocks = zmalloc(sizeof(struct rocks));
    char *err = NULL, dir[ROCKS_DIR_MAX_LEN];
    rocksdb_cache_t *block_cache;
    const char *default_cf_name = "default";

    rocks->snapshot = NULL;
    rocks->checkpoint = NULL;
    rocks->db_opts = rocksdb_options_create();
    rocksdb_options_set_create_if_missing(rocks->db_opts, 1); 
    /* enable stats might cause fork hang. */
    /* rocksdb_options_enable_statistics(rocks->db_opts); */
    /* rocksdb_options_set_stats_dump_period_sec(rocks->db_opts, 60); */
    rocksdb_options_set_max_write_buffer_number(rocks->db_opts, 6);
    struct rocksdb_block_based_table_options_t *block_opts = rocksdb_block_based_options_create();
    rocksdb_block_based_options_set_block_size(block_opts, 8*KB);
    block_cache = rocksdb_cache_create_lru(1*MB);
    rocks->block_cache = block_cache;
    rocksdb_block_based_options_set_block_cache(block_opts, block_cache);
    rocksdb_block_based_options_set_cache_index_and_filter_blocks(block_opts, 0);
    rocksdb_options_set_block_based_table_factory(rocks->db_opts, block_opts);
    rocks->block_opts = block_opts;
    rocksdb_options_optimize_for_point_lookup(rocks->db_opts, 1);

    /* rocksdb_options_optimize_level_style_compaction(rocks->db_opts, 256*1024*1024); */
    rocksdb_options_set_min_write_buffer_number_to_merge(rocks->db_opts, 2);
    rocksdb_options_set_max_write_buffer_number(rocks->db_opts, 6);
    rocksdb_options_set_level0_file_num_compaction_trigger(rocks->db_opts, 2);
    rocksdb_options_set_target_file_size_base(rocks->db_opts, 32*MB);
    rocksdb_options_set_max_bytes_for_level_base(rocks->db_opts, 256*MB);

    rocksdb_options_set_max_background_compactions(rocks->db_opts, 4); /* default 1 */
    rocksdb_options_compaction_readahead_size(rocks->db_opts, 2*1024*1024); /* default 0 */
    rocksdb_options_set_optimize_filters_for_hits(rocks->db_opts, 1); /* default false */

    rocksdb_options_set_compression(rocks->db_opts, server.rocksdb_compression);

    rocks->ropts = rocksdb_readoptions_create();
    rocksdb_readoptions_set_verify_checksums(rocks->ropts, 0);
    rocksdb_readoptions_set_fill_cache(rocks->ropts, 0);

    rocks->wopts = rocksdb_writeoptions_create();
    rocksdb_writeoptions_disable_WAL(rocks->wopts, 1);

    struct stat statbuf;
    if (!stat(ROCKS_DATA, &statbuf) && S_ISDIR(statbuf.st_mode)) {
        /* "data.rocks" folder already exists, remove it on start */
        rmdirRecursive(ROCKS_DATA);
    }
    if (mkdir(ROCKS_DATA, 0755)) {
        serverLog(LL_WARNING, "[ROCKS] mkdir %s failed: %s",
                ROCKS_DATA, strerror(errno));
        return -1;
    }

    snprintf(dir, ROCKS_DIR_MAX_LEN, "%s/%d", ROCKS_DATA, server.rocksdb_epoch);
    rocksdb_options_t *cf_opts[1];
    cf_opts[0] = rocks->db_opts;
    rocks->db = rocksdb_open_column_families(rocks->db_opts, dir, 1,
            &default_cf_name, (const rocksdb_options_t *const *)cf_opts,
            &rocks->default_cf, &err);
    if (err != NULL) {
        serverLog(LL_WARNING, "[ROCKS] rocksdb open failed: %s", err);
        return -1;
    }

    serverLog(LL_NOTICE, "[ROCKS] opened rocks data in (%s).", dir);
    server.rocks = rocks;
    return 0;
}

void rocksRelease() {
    char dir[ROCKS_DIR_MAX_LEN];
    snprintf(dir, ROCKS_DIR_MAX_LEN, "%s/%d", ROCKS_DATA, server.rocksdb_epoch);
    rocks *rocks = server.rocks;
    serverLog(LL_NOTICE, "[ROCKS] releasing rocksdb in (%s).",dir);
    rocksdb_cache_destroy(rocks->block_cache);
    rocksdb_block_based_options_destroy(rocks->block_opts);
    rocksdb_options_destroy(rocks->db_opts);
    rocksdb_writeoptions_destroy(rocks->wopts);
    rocksdb_readoptions_destroy(rocks->ropts);
    rocksReleaseSnapshot();
    rocksdb_close(rocks->db);
    zfree(rocks);
    server.rocks = NULL;
}

void rocksReinit() {
    rocksdb_checkpoint_t* checkpoint = server.rocks->checkpoint;
    sds checkpoint_dir = server.rocks->checkpoint_dir;
    server.rocks->checkpoint = NULL;
    server.rocks->checkpoint_dir = NULL;
    rocksRelease();
    server.rocksdb_epoch++;
    rocksInit();
    server.rocks->checkpoint = checkpoint;
    server.rocks->checkpoint_dir = checkpoint_dir;
}

void rocksCreateSnapshot() {
    rocks *rocks = server.rocks;
    if (rocks->snapshot) {
        serverLog(LL_WARNING, "[rocks] release snapshot before create.");
        rocksdb_release_snapshot(rocks->db, rocks->snapshot);
    }
    rocks->snapshot = rocksdb_create_snapshot(rocks->db);
    serverLog(LL_NOTICE, "[rocks] create rocksdb snapshot ok.");
}


void rocksReleaseCheckpoint() {
    rocks *rocks = server.rocks; 
    char* err = NULL;
    if (rocks->checkpoint != NULL) {
        serverLog(LL_NOTICE, "[rocks] releasing checkpoint in (%s).", rocks->checkpoint_dir);
        rocksdb_checkpoint_object_destroy(rocks->checkpoint);
        rocks->checkpoint = NULL;
        rocksdb_destroy_db(rocks->db_opts, rocks->checkpoint_dir, &err);
        if (err != NULL) {
            serverLog(LL_WARNING, "[rocks] destory db fail: %s", rocks->checkpoint_dir);
        }
        sdsfree(rocks->checkpoint_dir);
        rocks->checkpoint_dir = NULL;
    }
    
}

int rocksCreateCheckpoint(sds checkpoint_dir) {
    rocksdb_checkpoint_t* checkpoint = NULL;
    rocks *rocks = server.rocks; 
    if (rocks->checkpoint != NULL) {
        serverLog(LL_WARNING, "[rocks] release checkpoint before create.");
        rocksReleaseCheckpoint();
    }
    char* err = NULL;
    checkpoint = rocksdb_checkpoint_object_create(rocks->db, &err);
    if (err != NULL) {
        serverLog(LL_WARNING, "[rocks] checkpoint object create fail :%s\n", err);
        goto error;
    }
    rocksdb_checkpoint_create(checkpoint, checkpoint_dir, 0, &err);
    if (err != NULL) {
        serverLog(LL_WARNING, "[rocks] checkpoint %s create fail: %s", checkpoint_dir, err);
        goto error;
    }
    rocks->checkpoint = checkpoint;
    rocks->checkpoint_dir = checkpoint_dir;
    return 1;
error:
    if(checkpoint != NULL) {
        rocksdb_checkpoint_object_destroy(checkpoint);
    } 
    sdsfree(checkpoint_dir);
    return 0;
}

void rocksUseSnapshot() {
    rocks *rocks = server.rocks;
    if (rocks->snapshot) {
        rocksdb_readoptions_set_snapshot(rocks->ropts, rocks->snapshot);
        serverLog(LL_NOTICE, "[rocks] use snapshot read ok.");
    } else {
        serverLog(LL_WARNING, "[rocks] use snapshot read failed: snapshot not exists.");
    }
}

void rocksReleaseSnapshot() {
    rocks *rocks = server.rocks;
    if (rocks->snapshot) {
        serverLog(LL_NOTICE, "[rocks] relase snapshot ok.");
        rocksdb_release_snapshot(rocks->db, rocks->snapshot);
        rocks->snapshot = NULL;
    }
}

int rmdirRecursive(const char *path) {
	struct dirent *p;
	DIR *d = opendir(path);
	size_t path_len = strlen(path);
	int r = 0;

	if (d == NULL) return -1;

	while (!r && (p=readdir(d))) {
		int r2 = -1;
		char *buf;
		size_t len;
		struct stat statbuf;

		/* Skip the names "." and ".." as we don't want to recurse on them. */
		if (!strcmp(p->d_name, ".") || !strcmp(p->d_name, ".."))
			continue;

		len = path_len + strlen(p->d_name) + 2; 
		buf = zmalloc(len);

		snprintf(buf, len, "%s/%s", path, p->d_name);
		if (!stat(buf, &statbuf)) {
			if (S_ISDIR(statbuf.st_mode))
				r2 = rmdirRecursive(buf);
			else
				r2 = unlink(buf);
		}

		zfree(buf);
		r = r2;
	}
	closedir(d);

	if (!r) r = rmdir(path);

	return r;
}

int rocksFlushAll() {
    char odir[ROCKS_DIR_MAX_LEN];

    snprintf(odir,ROCKS_DIR_MAX_LEN,"%s/%d",ROCKS_DATA,server.rocksdb_epoch);
    asyncCompleteQueueDrain(-1);
    rocksReinit();
    rmdirRecursive(odir);
    serverLog(LL_NOTICE,"[ROCKS] remove rocks data in (%s).",odir);
    return 0;
}

rocksdb_t *rocksGetDb() {
    return server.rocks->db;
}

struct rocksdbMemOverhead *rocksGetMemoryOverhead() {
    rocksdbMemOverhead *mh;
    rocksdb_t *db;
    size_t total = 0, mem;

    if (server.rocks->db == NULL)
        return NULL;

    mh = zmalloc(sizeof(struct rocksdbMemOverhead));
    db = server.rocks->db;
    if (!rocksdb_property_int(db, "rocksdb.cur-size-all-mem-tables", &mem)) {
        mh->memtable = mem;
        total += mem;
    } else {
        mh->memtable = -1;
    }

    if (!rocksdb_property_int(db, "rocksdb.block-cache-usage", &mem)) {
        mh->block_cache = mem;
        total += mem;
    } else {
        mh->block_cache = -1;
    }

    if (!rocksdb_property_int(db, "rocksdb.estimate-table-readers-mem", &mem)) {
        mh->index_and_filter = mem;
        total += mem;
    } else {
        mh->index_and_filter = -1;
    }

    if (!rocksdb_property_int(db, "rocksdb.block-cache-pinned-usage", &mem)) {
        mh->pinned_blocks = mem;
        total += mem;
    } else {
        mh->pinned_blocks = -1;
    }

    mh->total = total;
    return mh;
}

void rocksFreeMemoryOverhead(struct rocksdbMemOverhead *mh) {
    if (mh) zfree(mh);
}

sds genRocksInfoKeyspaceString(sds info) {
	char *err;
	size_t used_db_size = 0, max_db_size = 0, disk_capacity = 0, used_disk_size = 0;
	size_t sequence = 0;
	float used_db_percent = 0, used_disk_percent = 0;
	rocksdb_t *db = server.rocks->db;
	const char *begin_key = "\x0", *end_key = "\xff";
	const size_t begin_key_len = 1, end_key_len = 1;
	struct statvfs stat;

	if (db) {
		sequence = rocksdb_get_latest_sequence_number(db);
		rocksdb_approximate_sizes(db,1,&begin_key,&begin_key_len,&end_key,&end_key_len,&used_db_size,&err);
		max_db_size = server.maxdisk;
		if (max_db_size) used_db_percent = (float)(used_db_size)/max_db_size;
	}

	if (statvfs(ROCKS_DATA, &stat) == 0) {
		disk_capacity = stat.f_blocks * stat.f_frsize;
		used_disk_size = (stat.f_blocks - stat.f_bavail) * stat.f_frsize;
		if (disk_capacity) used_disk_percent = (float)used_disk_size * 100 / disk_capacity;
	} 

	info = sdscatprintf(info,
			"sequence:%lu\r\n"
			"used_db_size:%lu\r\n"
			"max_db_size:%lu\r\n"
			"used_percent:%0.2f%%\r\n"
			"disk_capacity:%lu\r\n"
			"used_disk_size:%lu\r\n"
			"used_disk_percent:%0.2f%%\r\n",
			sequence,
			used_db_size,
			max_db_size,
			used_db_percent,
			disk_capacity,
			used_disk_size,
			used_disk_percent);

	return info;
}

#define ROCKSDB_DISK_USED_UPDATE_PERIOD 60
#define ROCKSDB_DISK_HEALTH_DETECT_PERIOD 1

void rocksCron() {
    static long long rocks_cron_loops = 0;
    char path[ROCKS_DIR_MAX_LEN] = {0};

    if (rocks_cron_loops % ROCKSDB_DISK_USED_UPDATE_PERIOD == 0) {
        uint64_t property_int = 0;
        if (!rocksdb_property_int(server.rocks->db,
                    "rocksdb.total-sst-files-size", &property_int)) {
            server.rocksdb_disk_used = property_int;
        }
        if (server.maxdisk && server.rocksdb_disk_used > server.maxdisk) {
            serverLog(LL_WARNING, "Rocksdb disk usage exceeds maxdisk %lld > %lld.",
                    server.rocksdb_disk_used, server.maxdisk);
        }
    }

    if (rocks_cron_loops % ROCKSDB_DISK_HEALTH_DETECT_PERIOD == 0) {
        snprintf(path,ROCKS_DIR_MAX_LEN,"%s/%s",
                ROCKS_DATA,ROCKS_DISK_HEALTH_DETECT_FILE);
        int disk_error = 0;
        FILE *fp = fopen(path,"w");
        if (fp == NULL) disk_error = 1;
        if (!disk_error && fprintf(fp,"%lld",server.mstime) < 0) disk_error = 1;
        if (!disk_error && fflush(fp)) disk_error = 1;
        if (disk_error) {
            if (!server.rocksdb_disk_error) {
                server.rocksdb_disk_error = 1;
                server.rocksdb_disk_error_since = server.mstime;
                serverLog(LL_WARNING,"Detected rocksdb disk failed: %s, %s",
                        path, strerror(errno));
            }
        } else {
            if (server.rocksdb_disk_error) {
                server.rocksdb_disk_error = 0;
                server.rocksdb_disk_error_since = 0;
                serverLog(LL_WARNING,"Detected rocksdb disk recovered.");
            }
        }
        if (fp) fclose(fp);
    }

    rocks_cron_loops++;
}
