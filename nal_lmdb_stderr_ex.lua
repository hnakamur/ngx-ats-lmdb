local lmdb = require "nal_lmdb_stderr"

local env_path = "/tmp/test_lmdb"
local max_databases = 20
local map_size = 50 * 1024 * 1024
local err = lmdb.env_init(env_path, max_databases, map_size)
print(string.format("env_init err=%s", err))

err = lmdb.with_txn(nil, function(txn)
    local db1, val, found
    db1, err = txn:db_open("db1")
    if err ~= nil then
        return err
    end

    val, found, err = txn:get("key1", db1)
    print(string.format("get#1 val=%s, found=%s, err=%s", val, found, err))
    if err ~= nil then
        return err
    end

    err = txn:put("key1", "value1", db1)
    print(string.format("put err=%s", err))
    if err ~= nil then
        return err
    end

    val, found, err = txn:get("key1", db1)
    print(string.format("get#2 val=%s, found=%s, err=%s", val, found, err))
    if err ~= nil then
        return err
    end

    return nil
end)
print(string.format("with_txn#1, err=%s", err))

err = lmdb.with_readonly_txn(nil, function(txn)
    local db1, val, found
    db1, err = txn:readonly_db_open("db1")
    if err ~= nil then
        return err
    end

    val, found, err = txn:get("key1", db1)
    print(string.format("txn#2 get#1 val=%s, found=%s, err=%s", val, found, err))
    if err ~= nil then
        return err
    end

    return nil
end)
print(string.format("with_readonly_txn#1, err=%s", err))

err = lmdb.with_txn(nil, function(txn)
    local db1, val, found
    db1, err = txn:db_open("db1")
    if err ~= nil then
        return err
    end

    err = txn:del("key1", db1)
    print(string.format("del err=%s", err))
    if err ~= nil then
        return err
    end

    return nil
end)
print(string.format("with_txn#2, err=%s", err))
