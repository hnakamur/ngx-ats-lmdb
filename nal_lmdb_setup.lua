local function setup(shlib_name)
    local ffi = require "ffi"
    local S = ffi.load(shlib_name)

    ffi.cdef[[
        int nal_env_init(const char *env_path, size_t max_databases, size_t map_size);

        typedef struct MDB_txn * nal_txn_ptr;
        typedef unsigned int	 nal_dbi;
        typedef struct nal_val {
            size_t	    mv_size;
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
    ]]

    local c_txn_ptr_type = ffi.typeof("nal_txn_ptr[1]")
    local c_dbi_type = ffi.typeof("nal_dbi[1]")
    local c_val_type = ffi.typeof("nal_val[1]")

    local MDB_SUCCESS = 0
    local MDB_NOTFOUND = -30798

    local function nal_strerror(err)
        return ffi.string(S.nal_strerror(err))
    end

    local function env_init(env_path, max_databases, map_size)
        local rc = S.nal_env_init(env_path, max_databases, map_size)
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

    local function txn_abort(txn)
        S.nal_txn_abort(txn)
    end

    local function db_open(txn, name)
        local dbi = ffi.new(c_dbi_type)
        local rc = S.nal_dbi_open(txn, name, dbi)
        if rc ~= MDB_SUCCESS then
            return nil, nal_strerror(rc)
        end
        return dbi[0]
    end

    local function readonly_db_open(txn, name)
        local dbi = ffi.new(c_dbi_type)
        local rc = S.nal_readonly_dbi_open(txn, name, dbi)
        if rc ~= MDB_SUCCESS then
            return nil, nal_strerror(rc)
        end
        return dbi[0]
    end

    local function nal_put(txn, dbi, key, data)
        local nal_key = ffi.new(c_val_type)
        local nal_data = ffi.new(c_val_type)
        nal_key[0].mv_size = #key
        nal_key[0].mv_data = key
        nal_data[0].mv_size = #data
        nal_data[0].mv_data = data
        local rc = S.nal_put(txn, dbi, nal_key, nal_data)
        if rc ~= MDB_SUCCESS then
            return nal_strerror(rc)
        end
        return nil
    end

    local function nal_del(txn, dbi, key)
        local nal_key = ffi.new(c_val_type)
        nal_key[0].mv_size = #key
        nal_key[0].mv_data = key
        local rc = S.nal_del(txn, dbi, nal_key)
        if rc ~= 0 then
            if rc == MDB_NOTFOUND then
                return false
            end
            return false, S.nal_strerror(rc)
        end
        return true
    end

    local function nal_get(txn, dbi, key)
        local nal_key = ffi.new(c_val_type)
        nal_key[0].mv_size = #key
        nal_key[0].mv_data = key
        local nal_data = ffi.new(c_val_type)
        local rc = S.nal_get(txn, dbi, nal_key, nal_data)
        if rc ~= 0 then
            if rc == MDB_NOTFOUND then
                return nil, false
            end
            return nil, false, S.nal_strerror(rc)
        end
        return ffi.string(nal_data[0].mv_data, nal_data[0].mv_size), true
    end

    local txn_mt = {}
    txn_mt.__index = txn_mt

    function txn_mt:db_open(name)
        return  db_open(self, name)
    end

    function txn_mt:readonly_db_open(name)
        return  readonly_db_open(self, name)
    end

    function txn_mt:get(key, dbi)
        return nal_get(self, dbi, key)
    end

    function txn_mt:put(key, data, dbi)
        return nal_put(self, dbi, key, data)
    end

    function txn_mt:del(key, dbi)
        return nal_del(self, dbi, key)
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
    
    local function with_readonly_txn(parent, f)
        local txn, err = readonly_txn_begin(parent)
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

    return {
        env_init = env_init,
        with_txn = with_txn,
        with_readonly_txn = with_readonly_txn,
    }
end

return setup
