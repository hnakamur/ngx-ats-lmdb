#ifndef NAL_LMDB_H
#define NAL_LMDB_H

#include <stddef.h>

int nal_env_init(const char *env_path, size_t max_databases, size_t map_size);

typedef struct MDB_txn *nal_txn_ptr;
typedef unsigned int nal_dbi;
typedef struct nal_val {
    size_t mv_size;
    const char *mv_data;
} nal_val;

const char *nal_strerror(int err);
int nal_txn_begin(nal_txn_ptr parent, nal_txn_ptr *txn);
int nal_readonly_txn_begin(nal_txn_ptr parent, nal_txn_ptr *txn);
int nal_txn_commit(nal_txn_ptr txn);
void nal_txn_abort(nal_txn_ptr txn);
int nal_dbi_open(nal_txn_ptr txn, const char *name, nal_dbi *dbi);
int nal_readonly_dbi_open(nal_txn_ptr txn, const char *name, nal_dbi *dbi);
int nal_put(nal_txn_ptr txn, nal_dbi dbi, nal_val *key, nal_val *data);
int nal_del(nal_txn_ptr txn, nal_dbi dbi, nal_val *key);
int nal_get(nal_txn_ptr txn, nal_dbi dbi, nal_val *key, nal_val *data);

#endif
