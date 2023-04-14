local function setup(shlib_name)
    local ffi = require "ffi"
    local S = ffi.load(shlib_name)

    ffi.cdef[[
        int nal_env_init(const char *env_path, size_t max_databases,
                         unsigned int max_readers, size_t map_size, uint32_t file_mode,
                         int use_tls);

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

    local function nal_strerror(err)
        return ffi.string(S.nal_strerror(err))
    end

    local function env_init(env_path, max_databases, max_readers, map_size, file_mode, use_tls)
        -- use 0 if use_tls is nil
        local rc = S.nal_env_init(env_path, max_databases, max_readers, map_size, file_mode, use_tls or 0)
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

    local function dbi_open(txn, name)
        local dbi = ffi.new(c_dbi_type)
        local rc = S.nal_dbi_open(txn, name, dbi)
        if rc ~= MDB_SUCCESS then
            return nil, nal_strerror(rc)
        end
        return dbi[0]
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

    local ro_txns = {}
    local dbis = {}

    local txn_mt = {}
    txn_mt.__index = txn_mt

    function txn_mt:get(key, db)
        local val, val_len, err = self:get_raw(key, #key, db)
        if val == nil then
            return nil, err
        end
        return ffi.string(val, val_len)
    end

    function txn_mt:get_raw(key, key_len, db)
        local nal_key = ffi.new(c_val_type)
        nal_key[0].mv_size = key_len
        nal_key[0].mv_data = key
        local nal_data = ffi.new(c_val_type)
        local rc = S.nal_get(self, dbis[db], nal_key, nal_data)
        if rc ~= 0 then
            if rc == MDB_NOTFOUND then
                return nil, 0
            end
            return nil, 0, nal_strerror(rc)
        end
        return nal_data[0].mv_data, nal_data[0].mv_size
    end

    function txn_mt:set(key, data, db)
        return self:set_raw(key, #key, data, #data, db)
    end

    function txn_mt:set_raw(key, key_len, data, data_len, db)
        local nal_key = ffi.new(c_val_type)
        local nal_data = ffi.new(c_val_type)
        nal_key[0].mv_size = key_len
        nal_key[0].mv_data = key
        nal_data[0].mv_size = data_len
        nal_data[0].mv_data = data
        local rc = S.nal_put(self, dbis[db], nal_key, nal_data)
        if rc ~= MDB_SUCCESS then
            return nal_strerror(rc)
        end
        return nil
    end

    function txn_mt:del(key, db)
        return self:del_raw(key, #key, db)
    end

    function txn_mt:del_raw(key, key_len, db)
        local nal_key = ffi.new(c_val_type)
        nal_key[0].mv_size = key_len
        nal_key[0].mv_data = key
        local rc = S.nal_del(self, dbis[db], nal_key)
        if rc ~= 0 and rc ~= MDB_NOTFOUND then
            return nal_strerror(rc)
        end
        return nil
    end

    ffi.metatype("struct MDB_txn", txn_mt)

    local function update(f)
        local txn, err = txn_begin(nil)
        if err ~= nil then
            return err
        end

        err = f(txn)
        if err ~= nil then
            S.nal_txn_abort(txn)
            return err
        end
        return txn_commit(txn)
    end

    local function get_ro_txn()
        local txn = table.remove(ro_txns)
        if txn ~= nil then
            local err = txn_renew(txn)
            if err ~= nil then
                return nil, err
            end
            return txn
        end

        return readonly_txn_begin(nil)
    end

    local function put_ro_txn(txn)
        S.nal_txn_reset(txn)
        table.insert(ro_txns, txn)
    end

    local function view(f)
        local txn, err = get_ro_txn()
        if err ~= nil then
            return err
        end

        err = f(txn)
        put_ro_txn(txn)
        return err
    end

    local function open_databases(databases)
        return update(function(txn)
            for i, db in ipairs(databases) do
                local dbi, err = dbi_open(txn, db)
                if err ~= nil then
                    return err
                end
                dbis[db] = dbi
            end
        end)
    end

    local function get(key, db)
        local val
        local err = view(function(txn)
            local err2
            val, err2 = txn:get(key, db)
            if err2 ~= nil then
                return err2
            end

            return nil
        end)
        return val, err
    end

    return {
        env_init = env_init,
        update = update,
        view = view,
        open_databases = open_databases,
        get = get,
    }
end

return setup
