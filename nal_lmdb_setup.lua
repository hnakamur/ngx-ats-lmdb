local function setup(shlib_name)
    local ffi = require "ffi"
    local S = ffi.load(shlib_name)

    ffi.cdef[[
        int nal_env_init(const char *env_path, size_t max_databases,
                         unsigned int max_readers, size_t map_size);

        typedef struct MDB_txn * nal_txn_ptr;
        typedef unsigned int     nal_dbi;
        typedef struct nal_val {
            size_t      mv_size;
            const char *mv_data;
        } nal_val;

        const char *nal_strerror(int err);
        int nal_txn_begin(nal_txn_ptr parent, nal_txn_ptr *txn);
        int nal_readonly_txn_begin(nal_txn_ptr parent, nal_txn_ptr *txn);
        int nal_txn_commit(nal_txn_ptr txn);
        void nal_txn_abort(nal_txn_ptr txn);
        int nal_txn_renew(nal_txn_ptr txn);
        void nal_txn_reset(nal_txn_ptr txn);
        int nal_dbi_open(nal_txn_ptr txn, const char *name, nal_dbi *dbi);
        int nal_readonly_dbi_open(nal_txn_ptr txn, const char *name, nal_dbi *dbi);
        int nal_put(nal_txn_ptr txn, nal_dbi dbi, nal_val *key, nal_val *data);
        int nal_del(nal_txn_ptr txn, nal_dbi dbi, nal_val *key);
        int nal_get(nal_txn_ptr txn, nal_dbi dbi, nal_val *key, nal_val *data);
    ]]

    local c_txn_ptr_type = ffi.typeof("nal_txn_ptr[1]")
    local c_dbi_type = ffi.typeof("nal_dbi[1]")
    local c_val_type = ffi.typeof("nal_val[1]")

    local MDB_SUCCESS = 0
    local MDB_NOTFOUND = -30798
    local EAGAIN = 11

    local function nal_strerror(err)
        return ffi.string(S.nal_strerror(err))
    end

    local function env_init(env_path, max_databases, max_readers, map_size)
        local rc = S.nal_env_init(env_path, max_databases, max_readers, map_size)
        if rc ~= MDB_SUCCESS then
            return nal_strerror(rc)
        end
        return nil
    end

    local function txn_begin(parent)
        local txn = ffi.new(c_txn_ptr_type)
        local rc = S.nal_txn_begin(parent, txn)
        if rc ~= MDB_SUCCESS then
            return nil, nal_strerror(rc)
        end
        return txn[0]
    end

    local function readonly_txn_begin(parent)
        local txn = ffi.new(c_txn_ptr_type)
        local rc = S.nal_readonly_txn_begin(parent, txn)
        if rc ~= MDB_SUCCESS then
            return nil, nal_strerror(rc)
        end
        return txn[0]
    end

    local function txn_commit(txn)
        local rc = S.nal_txn_commit(txn)
        if rc ~= MDB_SUCCESS then
            return nal_strerror(rc)
        end
        return nil
    end

    local function txn_renew(txn)
        local rc = S.nal_txn_renew(txn)
        if rc ~= MDB_SUCCESS then
            return nal_strerror(rc)
        end
        return nil
    end

    local function txn_abort(txn)
        S.nal_txn_abort(txn)
    end

    local txn_mt = {}
    txn_mt.__index = txn_mt

    function txn_mt:db_open(name)
        local dbi = ffi.new(c_dbi_type)
        local rc = S.nal_dbi_open(self, name, dbi)
        if rc ~= MDB_SUCCESS then
            return nil, nal_strerror(rc)
        end
        return dbi[0]
    end

    function txn_mt:readonly_db_open(name)
        local dbi = ffi.new(c_dbi_type)
        local rc = S.nal_readonly_dbi_open(self, name, dbi)
        if rc ~= MDB_SUCCESS then
            return nil, nal_strerror(rc)
        end
        return dbi[0]
    end

    function txn_mt:get(key, dbi)
        local nal_key = ffi.new(c_val_type)
        nal_key[0].mv_size = #key
        nal_key[0].mv_data = key
        local nal_data = ffi.new(c_val_type)
        local rc = S.nal_get(self, dbi, nal_key, nal_data)
        if rc ~= 0 then
            if rc == MDB_NOTFOUND then
                return nil
            end
            return nil, nal_strerror(rc)
        end
        return ffi.string(nal_data[0].mv_data, nal_data[0].mv_size)
    end

    function txn_mt:put(key, data, dbi)
        local nal_key = ffi.new(c_val_type)
        local nal_data = ffi.new(c_val_type)
        nal_key[0].mv_size = #key
        nal_key[0].mv_data = key
        nal_data[0].mv_size = #data
        nal_data[0].mv_data = data
        local rc = S.nal_put(self, dbi, nal_key, nal_data)
        if rc ~= MDB_SUCCESS then
            return nal_strerror(rc)
        end
        return nil
    end

    function txn_mt:del(key, dbi)
        local nal_key = ffi.new(c_val_type)
        nal_key[0].mv_size = #key
        nal_key[0].mv_data = key
        local rc = S.nal_del(self, dbi, nal_key)
        if rc ~= 0 and rc ~= MDB_NOTFOUND then
            return nal_strerror(rc)
        end
        return nil
    end

    ffi.metatype("struct MDB_txn", txn_mt)

    local function with_txn(parent, f)
        local txn, err = txn_begin(parent)
        if err ~= nil then
            return err
        end

        err = f(txn)
        if err ~= nil then
            txn_abort(txn)
            return err
        end
        return txn_commit(txn)
    end

    local ro_txns = {}
    local dbis = {}

    local function get_ro_txn()
        local i = table.maxn(ro_txns)
        if i ~= 0 then
            local txn = ro_txns[i]
            ro_txns[i] = nil
            local err = txn_renew(txn)
            if err ~= nil then
                return nil, err
            end
            return txn
        end

        return readonly_txn_begin(nil)
    end

    local function put_ro_txn(txn)
        table.insert(ro_txns, txn)
    end

    local function with_readonly_txn(f)
        local txn, err = get_ro_txn()
        if err ~= nil then
            return err
        end

        local err = f(txn)
        S.nal_txn_reset(txn)
        put_ro_txn(txn)
        return err
    end

    local function open_databases(databases)
        local txn, err = txn_begin(nil)
        if err ~= nil then
            return err
        end

        for i, db in ipairs(databases) do
            local dbi
            dbi, err = txn:db_open(db)
            if err ~= nil then
                return err
            end
            dbis[db] = dbi
        end

        err = txn_commit(txn)
        if err ~= nil then
            return err
        end

        return nil
    end

    local function get(key, db)
        local val
        local err = with_readonly_txn(function(txn)
            local err2
            val, err2 = txn:get(key, dbis[db])
            if err2 ~= nil then
                return err2
            end

            return nil
        end)
        return val, err
    end

    return {
        env_init = env_init,
        with_txn = with_txn,
        with_readonly_txn = with_readonly_txn,
        open_databases = open_databases,
        get = get,
    }
end

return setup
