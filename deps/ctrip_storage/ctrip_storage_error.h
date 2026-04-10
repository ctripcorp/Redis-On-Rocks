/*
 * ctrip_storage_error.h - swap 错误码定义（从 request.h 迁移）
 *
 * 按模块分组：SETUP(-1xx), DATA(-2xx), EXEC(-3xx), METASCAN(-4xx), RIO(-5xx)
 */
#ifndef __CTRIP_STORAGE_ERROR_H__
#define __CTRIP_STORAGE_ERROR_H__

/* ==================== Setup 错误 (-1xx) ==================== */
#define SWAP_ERR_SETUP_FAIL -100
#define SWAP_ERR_SETUP_UNEXPECTED_SWAP_TYPE -101
#define SWAP_ERR_SETUP_UNSUPPORTED -102

/* ==================== Data 错误 (-2xx) ==================== */
#define SWAP_ERR_DATA_FAIL -200
#define SWAP_ERR_DATA_ANA_FAIL -201
#define SWAP_ERR_DATA_DECODE_FAIL -202
#define SWAP_ERR_DATA_FIN_FAIL -203
#define SWAP_ERR_DATA_UNEXPECTED_INTENTION -204
#define SWAP_ERR_DATA_DECODE_META_FAILED -205
#define SWAP_ERR_DATA_WRONG_TYPE_ERROR -206

/* ==================== Exec 错误 (-3xx) ==================== */
#define SWAP_ERR_EXEC_FAIL -300
#define SWAP_ERR_EXEC_UNEXPECTED_ACTION -302
#define SWAP_ERR_EXEC_ROCKSDB_FLUSH_FAIL -303
#define SWAP_ERR_EXEC_UNEXPECTED_UTIL -304

/* ==================== MetaScan 错误 (-4xx) ==================== */
#define SWAP_ERR_METASCAN_FAIL -400
#define SWAP_ERR_METASCAN_UNSUPPORTED_IN_MULTI -401
#define SWAP_ERR_METASCAN_SESSION_UNASSIGNED -402
#define SWAP_ERR_METASCAN_SESSION_INPROGRESS -403
#define SWAP_ERR_METASCAN_SESSION_SEQUNMATCH -404

/* ==================== RIO 错误 (-5xx) ==================== */
#define SWAP_ERR_RIO_FAIL -500
#define SWAP_ERR_RIO_GET_FAIL -501
#define SWAP_ERR_RIO_PUT_FAIL -502
#define SWAP_ERR_RIO_DEL_FAIL -503
#define SWAP_ERR_RIO_ITER_FAIL -504
#define SWAP_ERR_RIO_OOM      -505

#endif /* __CTRIP_STORAGE_ERROR_H__ */