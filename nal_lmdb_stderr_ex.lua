local lmdb = require "nal_lmdb_stderr"

local env_path = "/tmp/test_lmdb"
local max_databases = 20
local max_readers = 128
local map_size = 50 * 1024 * 1024
local file_mode = tonumber('666', 8)
local use_tls = false
local err = lmdb.env_init(env_path, max_databases, max_readers, map_size, file_mode, use_tls)
print(string.format("env_init err=%s", err))

err = lmdb.open_databases({"db1"})
print(string.format("open_databases err=%s", err))

err = lmdb.update(function(txn)
    local val, err2 = txn:get("key1", "db1")
    print(string.format("get#1 val=%s, err=%s", val, err2))
    if err2 ~= nil then
        return err2
    end

    err2 = txn:set("key1", "value1", "db1")
    print(string.format("put err=%s", err2))
    if err2 ~= nil then
        return err2
    end

    val, err2 = txn:get("key1", "db1")
    print(string.format("get#2 val=%s, err=%s", val, err2))
    if err2 ~= nil then
        return err2
    end

    return nil
end)
print(string.format("update#1, err=%s", err))

local val
val, err = lmdb.get("key1", "db1")
print(string.format("lmdb.get val=%s, err=%s", val, err))

err = lmdb.update(function(txn)
    local err2 = txn:del("key1", "db1")
    print(string.format("del err=%s", err2))
    if err2 ~= nil then
        return err2
    end

    return nil
end)
print(string.format("update#2, err=%s", err))
