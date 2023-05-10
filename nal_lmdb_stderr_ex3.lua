local lmdb = require "nal_lmdb_stderr"
local ffi = require "ffi"

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

    local val1 = "value1"
    local val2 = "value2"
    val = '\006\0\0\0' .. val1 .. val2
    local key = "key1"
    err2 = txn:set_raw(key, #key, val, 4 + #val1 + #val2, "db1")
    print(string.format("put err=%s", err2))
    if err2 ~= nil then
        return err2
    end

    err2 = txn:set("key3", "value3", "db1")
    print(string.format("put key3 err=%s", err2))
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

err = lmdb.view(function(txn)
    return txn:with_cursor("db1", function(c)
        local found_key, val, err2 = c:get("key1", lmdb.SET_RANGE)
        if err2 ~= nil then
            return err2
        end
        print(string.format("cursor get#1, found_key=%s, val=%s", found_key, val))

        found_key, val, err2 = c:get("key2", lmdb.SET_RANGE)
        if err2 ~= nil then
            return err2
        end
        print(string.format("cursor get#2, found_key=%s, val=%s", found_key, val))

        found_key, val, err2 = c:get("key4", lmdb.SET_RANGE)
        if err2 ~= nil then
            return err2
        end
        print(string.format("cursor get#3, found_key=%s, val=%s, found_key is nil=%s", found_key, val, found_key == nil))
        return nil
    end)
end)
print(string.format("view, err=%s", err))
