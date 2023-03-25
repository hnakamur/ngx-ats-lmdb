local lmdb = require "nal_lmdb_stderr"

local env_path = "/tmp/test_lmdb"
local max_databases = 20
local map_size = 50 * 1024 * 1024
local err = lmdb.env_init(env_path, max_databases, map_size)
print(string.format("env_init err=%s", err))

local txn
txn, err = lmdb.txn_begin(nil)
print(string.format("txn_begin txn=%s, err=%s", txn, err))

local dbi
dbi, err = lmdb.db_open(txn, "db1")
print(string.format("dbi=%s, err=%s", dbi, err))

local val, found
val, found, err = lmdb.nal_get(txn, dbi, "key1")
print(string.format("get#1 val=%s, found=%s, err=%s", val, found, err))

err = lmdb.nal_put(txn, dbi, "key1", "value1")
print(string.format("put err=%s", err))

val, found, err = lmdb.nal_get(txn, dbi, "key1")
print(string.format("get#2 val=%s, found=%s, err=%s", val, found, err))

found, err = lmdb.nal_del(txn, dbi, "key1")
print(string.format("del#1 found=%s, err=%s", found, err))

val, found, err = lmdb.nal_get(txn, dbi, "key1")
print(string.format("get#3 val=%s, found=%s, err=%s", val, found, err))

found, err = lmdb.nal_del(txn, dbi, "key1")
print(string.format("del#2 found=%s, err=%s", found, err))

err = lmdb.txn_commit(txn)
print(string.format("txn_commit err=%s", err))
