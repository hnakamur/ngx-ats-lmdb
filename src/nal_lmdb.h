#ifndef NAL_LMDB_H
#define NAL_LMDB_H

#include <stddef.h>
#include <stdint.h>
#include <lmdb.h>

typedef struct MDB_txn *nal_txn_ptr;
typedef struct MDB_cursor *nal_cursor_ptr;

int nal_env_init(const char *env_path, size_t max_databases,
                 unsigned int max_readers, size_t map_size, uint32_t file_mode,
                 int use_tls);

const char *nal_strerror(int err);

int nal_txn_begin(nal_txn_ptr parent, nal_txn_ptr *txn);
int nal_readonly_txn_begin(nal_txn_ptr parent, nal_txn_ptr *txn);
int nal_txn_commit(nal_txn_ptr txn);
void nal_txn_abort(nal_txn_ptr txn);
int nal_txn_renew(nal_txn_ptr txn);
void nal_txn_reset(nal_txn_ptr txn);
int nal_dbi_open(nal_txn_ptr txn, const char *name, MDB_dbi *dbi);
int nal_readonly_dbi_open(nal_txn_ptr txn, const char *name, MDB_dbi *dbi);
int nal_put(nal_txn_ptr txn, MDB_dbi dbi, MDB_val *key, MDB_val *data);
int nal_del(nal_txn_ptr txn, MDB_dbi dbi, MDB_val *key);
int nal_get(nal_txn_ptr txn, MDB_dbi dbi, MDB_val *key, MDB_val *data);

int nal_cursor_open(nal_txn_ptr txn, MDB_dbi dbi, nal_cursor_ptr *cursor);
void nal_cursor_close(nal_cursor_ptr cursor);
int nal_cursor_get(nal_cursor_ptr cursor, MDB_val *key, MDB_val *data,
                   MDB_cursor_op op);
int nal_cursor_put(nal_cursor_ptr cursor, MDB_val *key, MDB_val *data,
                   unsigned int flags);
int nal_cursor_del(nal_cursor_ptr cursor, unsigned int flags);

#endif
