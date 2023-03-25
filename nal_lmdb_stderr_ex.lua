local lmdb = require "nal_lmdb_stderr"

local env_path = "/tmp/test_lmdb"
local max_databases = 20
local map_size = 50 * 1024 * 1024
local rc = lmdb.env_init(env_path, max_databases, map_size)
print(string.format("env_init rc=%d", rc))

local txn
txn, rc = lmdb.txn_begin(nil)
print(string.format("txn_begin txn=%s, rc=%s", txn, rc))

local dbi
dbi, rc = lmdb.db_open(txn, "db1")
print(string.format("dbi=%s, rc=%s", dbi, rc))

local val, found
val, found, rc = lmdb.nal_get(txn, dbi, "key1")
print(string.format("get#1 val=%s, found=%s, rc=%s", val, found, rc))

rc = lmdb.nal_put(txn, dbi, "key1", "value1")
print(string.format("put rc=%s", rc))

val, found, rc = lmdb.nal_get(txn, dbi, "key1")
print(string.format("get#2 val=%s, found=%s, rc=%s", val, found, rc))

found, rc = lmdb.nal_del(txn, dbi, "key1")
print(string.format("del#1 found=%s, rc=%s", found, rc))

val, found, rc = lmdb.nal_get(txn, dbi, "key1")
print(string.format("get#3 val=%s, found=%s, rc=%s", val, found, rc))

found, rc = lmdb.nal_del(txn, dbi, "key1")
print(string.format("del#2 found=%s, rc=%s", found, rc))

rc = lmdb.txn_commit(txn)
print(string.format("txn_commit rc=%s", rc))
