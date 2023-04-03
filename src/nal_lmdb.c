#include "nal_lmdb.h"

#include <lmdb.h>
#include <pthread.h>

#include "nal_log.h"

typedef struct nal_env_s {
    const char *env_path;
    size_t map_size;
    size_t max_databases;
    unsigned int max_readers;
    mdb_mode_t file_mode;
    int use_tls;
    MDB_env *env;
} nal_env_t;

static pthread_once_t env_init_once = PTHREAD_ONCE_INIT;
static int env_init_rc;
static nal_env_t env;

static void nal_do_init_env(void)
{
    int rc = mdb_env_create(&env.env);
    if (rc != 0) {
        nal_log_error("mdb_env_create failed: %s", mdb_strerror(rc));
        goto exit;
    }

    rc = mdb_env_set_maxdbs(env.env, env.max_databases);
    if (rc != 0) {
        nal_log_error("mdb_env_set_maxdbs failed: %s", mdb_strerror(rc));
        goto exit;
    }

    rc = mdb_env_set_maxreaders(env.env, env.max_readers);
    if (rc != 0) {
        nal_log_error("mdb_env_set_maxreaders failed: %s", mdb_strerror(rc));
        goto exit;
    }

    rc = mdb_env_set_mapsize(env.env, env.map_size);
    if (rc != 0) {
        nal_log_error("mdb_env_set_mapsize failed: %s", mdb_strerror(rc));
        goto exit;
    }

    unsigned int flags = env.use_tls ? 0 : MDB_NOTLS;
    rc = mdb_env_open(env.env, env.env_path, flags, env.file_mode);
    if (rc != 0) {
        nal_log_error("mdb_env_open failed: %s", mdb_strerror(rc));
        goto exit;
    }

    int dead = 0;
    rc = mdb_reader_check(env.env, &dead);
    if (rc != 0) {
        nal_log_error("mdb_reader_check failed: %s", mdb_strerror(rc));
    } else if (dead > 0) {
        nal_log_warning("found and cleared %d stale readers from LMDB", dead);
    }

exit:
    nal_log_note("nal_do_init_env exit: use_tls=%d, rc=%d", env.use_tls, rc);
    env_init_rc = rc;
}

int nal_env_init(const char *env_path, size_t max_databases,
                 unsigned int max_readers, size_t map_size, uint32_t file_mode,
                 int use_tls)
{
    env.env_path = env_path;
    env.max_databases = max_databases;
    env.max_readers = max_readers;
    env.map_size = map_size;
    env.file_mode = (mdb_mode_t)file_mode;
    env.use_tls = use_tls;
    (void)pthread_once(&env_init_once, nal_do_init_env);
    return env_init_rc;
}

const char *nal_strerror(int err)
{
    return mdb_strerror(err);
}

int nal_txn_begin(nal_txn_ptr parent, nal_txn_ptr *txn)
{
    MDB_txn *out;
    int rc = mdb_txn_begin(env.env, (MDB_txn *)parent, 0, &out);
    *txn = out;
    return rc;
}

int nal_readonly_txn_begin(nal_txn_ptr parent, nal_txn_ptr *txn)
{
    MDB_txn *out;
    int rc = mdb_txn_begin(env.env, (MDB_txn *)parent, MDB_RDONLY, &out);
    *txn = out;
    return rc;
}

int nal_txn_commit(nal_txn_ptr txn)
{
    return mdb_txn_commit(txn);
}

void nal_txn_abort(nal_txn_ptr txn)
{
    mdb_txn_abort(txn);
}

int nal_txn_renew(nal_txn_ptr txn)
{
    return mdb_txn_renew(txn);
}

void nal_txn_reset(nal_txn_ptr txn)
{
    mdb_txn_reset(txn);
}

int nal_dbi_open(nal_txn_ptr txn, const char *name, nal_dbi *dbi)
{
    return mdb_dbi_open(txn, name, MDB_CREATE, (MDB_dbi *)dbi);
}

int nal_readonly_dbi_open(nal_txn_ptr txn, const char *name, nal_dbi *dbi)
{
    return mdb_dbi_open(txn, name, 0, (MDB_dbi *)dbi);
}

int nal_put(nal_txn_ptr txn, nal_dbi dbi, nal_val *key, nal_val *data)
{
    return mdb_put(txn, (MDB_dbi)dbi, (MDB_val *)key, (MDB_val *)data, 0);
}

int nal_del(nal_txn_ptr txn, nal_dbi dbi, nal_val *key)
{
    return mdb_del(txn, (MDB_dbi)dbi, (MDB_val *)key, (MDB_val *)NULL);
}

int nal_get(nal_txn_ptr txn, nal_dbi dbi, nal_val *key, nal_val *data)
{
    return mdb_get(txn, (MDB_dbi)dbi, (MDB_val *)key, (MDB_val *)data);
}
