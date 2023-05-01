local function setup(shlib_name)
    local ffi = require "ffi"
    local S = ffi.load(shlib_name)

    ffi.cdef[[
        int nal_env_init(const char *env_path, size_t max_databases,
                         unsigned int max_readers, size_t map_size, uint32_t file_mode,
                         int use_tls);

        typedef unsigned int     MDB_dbi;
        typedef struct MDB_val {
            size_t      mv_size;
            const char *mv_data;
        } MDB_val;

        typedef enum MDB_cursor_op {
            MDB_FIRST,				/**< Position at first key/data item */
            MDB_FIRST_DUP,			/**< Position at first data item of current key.
                                        Only for #MDB_DUPSORT */
            MDB_GET_BOTH,			/**< Position at key/data pair. Only for #MDB_DUPSORT */
            MDB_GET_BOTH_RANGE,		/**< position at key, nearest data. Only for #MDB_DUPSORT */
            MDB_GET_CURRENT,		/**< Return key/data at current cursor position */
            MDB_GET_MULTIPLE,		/**< Return up to a page of duplicate data items
                                        from current cursor position. Move cursor to prepare
                                        for #MDB_NEXT_MULTIPLE. Only for #MDB_DUPFIXED */
            MDB_LAST,				/**< Position at last key/data item */
            MDB_LAST_DUP,			/**< Position at last data item of current key.
                                        Only for #MDB_DUPSORT */
            MDB_NEXT,				/**< Position at next data item */
            MDB_NEXT_DUP,			/**< Position at next data item of current key.
                                        Only for #MDB_DUPSORT */
            MDB_NEXT_MULTIPLE,		/**< Return up to a page of duplicate data items
                                        from next cursor position. Move cursor to prepare
                                        for #MDB_NEXT_MULTIPLE. Only for #MDB_DUPFIXED */
            MDB_NEXT_NODUP,			/**< Position at first data item of next key */
            MDB_PREV,				/**< Position at previous data item */
            MDB_PREV_DUP,			/**< Position at previous data item of current key.
                                        Only for #MDB_DUPSORT */
            MDB_PREV_NODUP,			/**< Position at last data item of previous key */
            MDB_SET,				/**< Position at specified key */
            MDB_SET_KEY,			/**< Position at specified key, return key + data */
            MDB_SET_RANGE,			/**< Position at first key greater than or equal to specified key. */
            MDB_PREV_MULTIPLE		/**< Position at previous page and return up to
                                        a page of duplicate data items. Only for #MDB_DUPFIXED */
        } MDB_cursor_op;

        typedef struct MDB_txn *nal_txn_ptr;
        typedef struct MDB_cursor *nal_cursor_ptr;

        int nal_env_init(const char *env_path, size_t max_databases,
                         unsigned int max_readers, size_t map_size, uint32_t file_mode,
                         int use_tls);

        const char *nal_strerror(int err);

        int nal_txn_begin(nal_txn_ptr parent, nal_txn_ptr *txn);
        int nal_readonly_txn_begin(nal_txn_ptr parent, nal_txn_ptr *txn);
        int nal_txn_commit(nal_txn_ptr txn);
        void nal_txn_abort(nal_txn_ptr txn);
        int nal_txn_renew(nal_txn_ptr txn);
        void nal_txn_reset(nal_txn_ptr txn);
        int nal_dbi_open(nal_txn_ptr txn, const char *name, MDB_dbi *dbi);
        int nal_readonly_dbi_open(nal_txn_ptr txn, const char *name, MDB_dbi *dbi);
        int nal_put(nal_txn_ptr txn, MDB_dbi dbi, MDB_val *key, MDB_val *data);
        int nal_del(nal_txn_ptr txn, MDB_dbi dbi, MDB_val *key);
        int nal_get(nal_txn_ptr txn, MDB_dbi dbi, MDB_val *key, MDB_val *data);

        int nal_cursor_open(nal_txn_ptr txn, MDB_dbi dbi, nal_cursor_ptr *cursor);
        void nal_cursor_close(nal_cursor_ptr cursor);
        int nal_cursor_get(nal_cursor_ptr cursor, MDB_val *key, MDB_val *data,
                           MDB_cursor_op op);
        int nal_cursor_put(nal_cursor_ptr cursor, MDB_val *key, MDB_val *data,
                           unsigned int flags);
        int nal_cursor_del(nal_cursor_ptr cursor, unsigned int flags);
    ]]

    local c_txn_ptr_type = ffi.typeof("nal_txn_ptr[1]")
    local c_dbi_type = ffi.typeof("MDB_dbi[1]")
    local c_val_type = ffi.typeof("MDB_val[1]")
    local c_cursor_ptr_type = ffi.typeof("nal_cursor_ptr[1]")

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

    function txn_mt:open_cursor(db)
        local cursor = ffi.new(c_cursor_ptr_type)
        local rc = S.nal_cursor_open(self, dbis[db], cursor)
        if rc ~= MDB_SUCCESS then
            return nil, nal_strerror(rc)
        end
        return cursor[0]
    end

    function txn_mt:with_cursor(db, f)
        local cursor, err = self:open_cursor(db)
        if cursor == nil then
            return nil, err
        end
        err = f(cursor)
        cursor:close()
        return err
    end

    ffi.metatype("struct MDB_txn", txn_mt)

    local cursor_mt = {}
    cursor_mt.__index = cursor_mt

    function cursor_mt:close()
        S.nal_cursor_close(self)
    end

    function cursor_mt:get(key, op)
        local found_key, found_key_len, val, val_len, err = self:get_raw(key, #key, op)
        if found_key == nil then
            return nil, nil, err
        end
        return ffi.string(found_key, found_key_len), ffi.string(val, val_len)
    end

    function cursor_mt:get_raw(key, key_len, op)
        local nal_key = ffi.new(c_val_type)
        nal_key[0].mv_size = key_len
        nal_key[0].mv_data = key
        local nal_data = ffi.new(c_val_type)
        local rc = S.nal_cursor_get(self, nal_key, nal_data, op)
        if rc ~= 0 then
            if rc == MDB_NOTFOUND then
                return nil, 0, nil, 0
            end
            return nil, 0, nil, 0, nal_strerror(rc)
        end
        return nal_key[0].mv_data, nal_key[0].mv_size, nal_data[0].mv_data, nal_data[0].mv_size
    end

    function cursor_mt:set(key, data, flags)
        return self:set_raw(key, #key, data, #data, flags)
    end

    function cursor_mt:set_raw(key, key_len, data, data_len, flags)
        local nal_key = ffi.new(c_val_type)
        local nal_data = ffi.new(c_val_type)
        nal_key[0].mv_size = key_len
        nal_key[0].mv_data = key
        nal_data[0].mv_size = data_len
        nal_data[0].mv_data = data
        local rc = S.nal_cursor_put(self, nal_key, nal_data, flags)
        if rc ~= MDB_SUCCESS then
            return nal_strerror(rc)
        end
        return nil
    end

    function cursor_mt:del(key, flags)
        return self:del_raw(key, #key, flags)
    end

    function cursor_mt:del_raw(key, key_len, flags)
        local nal_key = ffi.new(c_val_type)
        nal_key[0].mv_size = key_len
        nal_key[0].mv_data = key
        local rc = S.nal_cursor_del(self, nal_key, flags)
        if rc ~= 0 and rc ~= MDB_NOTFOUND then
            return nal_strerror(rc)
        end
        return nil
    end

    ffi.metatype("struct MDB_cursor", cursor_mt)

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

        -- cursor operations
        SET_RANGE = ffi.new("MDB_cursor_op", S.MDB_SET_RANGE),
    }
end

return setup
