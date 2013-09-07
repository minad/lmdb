#include "lmdb_ext.h"

static void check(int code) {
        if (!code)
                return;

        const char* err = mdb_strerror(code);
        const char* sep = strchr(err, ':');
        if (sep)
                err = sep + 2;

#define ERROR(name) if (code == MDB_##name) rb_raise(cError_##name, "%s", err);
#include "errors.h"
#undef ERROR

        rb_raise(cError, "%s", err); /* fallback */
}

static void transaction_deref(Transaction* transaction) {
        if (--transaction->refcount == 0) {
                Environment* env = (Environment*)DATA_PTR(transaction->env);
                environment_deref(env);
                if (!NIL_P(transaction->parent)) {
                        Transaction* parent = (Transaction*)DATA_PTR(transaction->parent);
                        transaction_deref(parent);
                }
                if (transaction->txn)
                        mdb_txn_abort(transaction->txn);
                free(transaction);
        }
}

static void transaction_mark(Transaction* transaction) {
        rb_gc_mark(transaction->parent);
        rb_gc_mark(transaction->env);
}

static VALUE transaction_commit(VALUE self) {
        TRANSACTION(self, transaction);
        ENVIRONMENT(transaction->env, environment);
        if (!transaction->txn)
                rb_raise(cError, "Transaction is terminated");

        // Check nesting
        VALUE p = environment->txn;
        while (!NIL_P(p) && p != self) {
                TRANSACTION(p, txn);
                p = txn->parent;
        }
        if (p != self)
                rb_raise(cError, "Transaction is not active");

        mdb_txn_commit(transaction->txn);

        p = environment->txn;
        while (p != self) {
                TRANSACTION(p, txn);
                txn->txn = 0;
                p = txn->parent;
        }
        transaction->txn = 0;

        environment->txn = transaction->parent;
        return Qnil;
}

static VALUE transaction_abort(VALUE self) {
        TRANSACTION(self, transaction);
        ENVIRONMENT(transaction->env, environment);

        // Check nesting
        VALUE p = environment->txn;
        while (!NIL_P(p) && p != self) {
                TRANSACTION(p, txn);
                p = txn->parent;
        }
        if (p != self)
                rb_raise(cError, "Transaction is not active");

        if (!transaction->txn)
                rb_raise(cError, "Transaction is terminated");
        mdb_txn_abort(transaction->txn);

        p = environment->txn;
        while (p != self) {
                TRANSACTION(p, txn);
                txn->txn = 0;
                p = txn->parent;
        } while (p != self);
        transaction->txn = 0;

        environment->txn = transaction->parent;
        return Qnil;
}

static VALUE call_with_transaction_helper(VALUE arg) {
        HelperArgs* a = (HelperArgs*)arg;
        return rb_funcall_passing_block(a->self, rb_intern(a->name), a->argc, a->argv);
}

static VALUE call_with_transaction(VALUE venv, VALUE self, const char* name, int argc, const VALUE* argv, int flags) {
        HelperArgs arg = { self, name, argc, argv };
        return with_transaction(venv, call_with_transaction_helper, (VALUE)&arg, flags);
}

static VALUE with_transaction(VALUE venv, VALUE(*fn)(VALUE), VALUE arg, int flags) {
        ENVIRONMENT(venv, environment);

        MDB_txn* txn;
        check(mdb_txn_begin(environment->env, environment_get_txn(venv), flags, &txn));

        Transaction* transaction;
        VALUE vtxn = Data_Make_Struct(cTransaction, Transaction, transaction_mark, transaction_deref, transaction);
        transaction->refcount = 1;
        transaction->parent = environment->txn;
        transaction->env = venv;
        transaction->txn = txn;
        environment->txn = vtxn;

        int exception;
        VALUE result = rb_protect(fn, NIL_P(arg) ? vtxn : arg, &exception);

        if (exception) {
                if (vtxn == environment->txn)
                        transaction_abort(vtxn);
                rb_jump_tag(exception);
        }
        if (vtxn == environment->txn)
                transaction_commit(vtxn);
        return result;
}

static void environment_check(Environment* environment) {
        if (!environment->env)
                rb_raise(cError, "Environment is closed");
}

static void environment_deref(Environment *environment) {
        if (--environment->refcount == 0) {
                if (environment->env)
                        mdb_env_close(environment->env);
                free(environment);
        }
}

static VALUE environment_close(VALUE self) {
        ENVIRONMENT(self, environment);
        mdb_env_close(environment->env);
        environment->env = 0;
        return Qnil;
}

static VALUE stat2hash(const MDB_stat* stat) {
        VALUE result = rb_hash_new();

#define STAT_SET(name) rb_hash_aset(result, ID2SYM(rb_intern(#name)), INT2NUM(stat->ms_##name))
        STAT_SET(psize);
        STAT_SET(depth);
        STAT_SET(branch_pages);
        STAT_SET(leaf_pages);
        STAT_SET(overflow_pages);
        STAT_SET(entries);
#undef STAT_SET

        return result;
}

static VALUE environment_stat(VALUE self) {
        ENVIRONMENT(self, environment);
        MDB_stat stat;
        check(mdb_env_stat(environment->env, &stat));
        return stat2hash(&stat);
}

static VALUE environment_info(VALUE self) {
        MDB_envinfo info;

        ENVIRONMENT(self, environment);
        check(mdb_env_info(environment->env, &info));

        VALUE result = rb_hash_new();

#define INFO_SET(name) rb_hash_aset(result, ID2SYM(rb_intern(#name)), INT2NUM((size_t)info.me_##name));
        INFO_SET(mapaddr);
        INFO_SET(mapsize);
        INFO_SET(last_pgno);
        INFO_SET(last_txnid);
        INFO_SET(maxreaders);
        INFO_SET(numreaders);
#undef INFO_SET

        return result;
}

static VALUE environment_copy(VALUE self, VALUE path) {
        ENVIRONMENT(self, environment);
        check(mdb_env_copy(environment->env, StringValueCStr(path)));
        return Qnil;
}

static VALUE environment_sync(int argc, VALUE *argv, VALUE self) {
        ENVIRONMENT(self, environment);

        VALUE force;
        rb_scan_args(argc, argv, "01", &force);

        check(mdb_env_sync(environment->env, RTEST(force)));
        return Qnil;
}

static VALUE environment_open(int argc, VALUE *argv, VALUE klass) {
        VALUE path, options;
        rb_scan_args(argc, argv, "11", &path, &options);

        int flags = 0, maxreaders = -1, maxdbs = 10;
        size_t mapsize = 0;
        mode_t mode = 0755;
        if (!NIL_P(options)) {
                VALUE value = rb_hash_aref(options, ID2SYM(rb_intern("flags")));
                if (!NIL_P(value))
                        flags = NUM2INT(value);

                value = rb_hash_aref(options, ID2SYM(rb_intern("mode")));
                if (!NIL_P(value))
                        mode = NUM2INT(value);

                value = rb_hash_aref(options, ID2SYM(rb_intern("maxreaders")));
                if (!NIL_P(value))
                        maxreaders = NUM2INT(value);

                value = rb_hash_aref(options, ID2SYM(rb_intern("maxdbs")));
                if (!NIL_P(value))
                        maxdbs = NUM2INT(value);

                value = rb_hash_aref(options, ID2SYM(rb_intern("mapsize")));
                if (!NIL_P(value))
                        mapsize = NUM2SIZET(value);
        }

        MDB_env* env;
        check(mdb_env_create(&env));

        Environment* environment;
        VALUE venv = Data_Make_Struct(cEnvironment, Environment, 0, environment_deref, environment);
        environment->env = env;
        environment->refcount = 1;
        environment->txn = Qnil;

        if (maxreaders > 0)
                check(mdb_env_set_maxreaders(environment->env, maxreaders));
        if (mapsize > 0)
                check(mdb_env_set_mapsize(environment->env, mapsize));

        check(mdb_env_set_maxdbs(environment->env, maxdbs <= 0 ? 1 : maxdbs));
        check(mdb_env_open(environment->env, StringValueCStr(path), flags, mode));
        if (rb_block_given_p())
                return rb_ensure(rb_yield, venv, environment_close, venv);

        return venv;
}

static VALUE environment_flags(VALUE self) {
        unsigned int flags;
        ENVIRONMENT(self, environment);
        check(mdb_env_get_flags(environment->env, &flags));
        return INT2NUM(flags & ENV_FLAGS);
}

static VALUE environment_path(VALUE self) {
        const char* path;
        ENVIRONMENT(self, environment);
        check(mdb_env_get_path(environment->env, &path));
        return rb_str_new2(path);
}

static VALUE environment_set_flags(VALUE self, VALUE vflags) {
        unsigned int flags = NUM2INT(vflags), oldflags;
        ENVIRONMENT(self, environment);
        check(mdb_env_get_flags(environment->env, &oldflags));
        check(mdb_env_set_flags(environment->env, oldflags & ENV_FLAGS, 0));
        check(mdb_env_set_flags(environment->env, flags, 1));
        return environment_flags(self);
}

static MDB_txn* environment_get_txn(VALUE self) {
        ENVIRONMENT(self, environment);
        if (NIL_P(environment->txn))
                return 0;
        TRANSACTION(environment->txn, transaction);
        if (!transaction->txn)
                rb_raise(cError, "Transaction is terminated");
        return transaction->txn;
}

static MDB_txn* environment_need_txn(VALUE self) {
        MDB_txn* txn = environment_get_txn(self);
        if (!txn)
                rb_raise(cError, "No active transaction");
        return txn;
}

static VALUE environment_transaction(int argc, VALUE *argv, VALUE self) {
        rb_need_block();

        VALUE readonly;
        rb_scan_args(argc, argv, "01", &readonly);
        unsigned int flags = RTEST(readonly) ? MDB_RDONLY : 0;

        return with_transaction(self, rb_yield, Qnil, flags);
}

static void database_deref(Database* database) {
        if (--database->refcount == 0) {
                Environment* env = (Environment*)DATA_PTR(database->env);
                environment_deref(env);
                free(database);
        }
}

static void database_mark(Database* database) {
        rb_gc_mark(database->env);
}

static VALUE environment_database(int argc, VALUE *argv, VALUE self) {
        ENVIRONMENT(self, environment);
        if (NIL_P(environment->txn))
                return call_with_transaction(self, self, "database", argc, argv, 0);

        VALUE vname, vflags;
        rb_scan_args(argc, argv, "02", &vname, &vflags);

        MDB_dbi dbi;
        check(mdb_dbi_open(environment_need_txn(self), NIL_P(vname) ? 0 : StringValueCStr(vname), NIL_P(vflags) ? 0 : NUM2INT(vflags), &dbi));

        Database* database;
        VALUE vdb = Data_Make_Struct(cDatabase, Database, database_mark, database_deref, database);
        database->dbi = dbi;
        database->env = self;
        database->refcount = 1;
        ++environment->refcount;

        return vdb;
}

static VALUE database_stat(VALUE self) {
        DATABASE(self, database);
        if (!environment_get_txn(database->env))
                return call_with_transaction(database->env, self, "stat", 0, 0, MDB_RDONLY);

        MDB_stat stat;
        check(mdb_stat(environment_need_txn(database->env), database->dbi, &stat));
        return stat2hash(&stat);
}

static VALUE database_drop(VALUE self) {
        DATABASE(self, database);
        if (!environment_get_txn(database->env))
                return call_with_transaction(database->env, self, "drop", 0, 0, 0);
        check(mdb_drop(environment_need_txn(database->env), database->dbi, 1));
        return Qnil;
}

static VALUE database_clear(VALUE self) {
        DATABASE(self, database);
        if (!environment_get_txn(database->env))
                return call_with_transaction(database->env, self, "clear", 0, 0, 0);
        check(mdb_drop(environment_need_txn(database->env), database->dbi, 0));
        return Qnil;
}

static VALUE database_get(VALUE self, VALUE vkey) {
        DATABASE(self, database);
        if (!environment_get_txn(database->env))
                return call_with_transaction(database->env, self, "get", 1, &vkey, MDB_RDONLY);

        vkey = StringValue(vkey);
        MDB_val key, value;
        key.mv_size = RSTRING_LEN(vkey);
        key.mv_data = RSTRING_PTR(vkey);

        int ret = mdb_get(environment_need_txn(database->env), database->dbi, &key, &value);
        if (ret == MDB_NOTFOUND)
                return Qnil;
        check(ret);
        return rb_str_new(value.mv_data, value.mv_size);
}

static VALUE database_put(int argc, VALUE *argv, VALUE self) {
        DATABASE(self, database);
        if (!environment_get_txn(database->env))
                return call_with_transaction(database->env, self, "put", argc, argv, 0);

        VALUE vkey, vflags, vval;
        rb_scan_args(argc, argv, "21", &vkey, &vval, &vflags);

        vkey = StringValue(vkey);
        vval = StringValue(vval);

        MDB_val key, value;
        key.mv_size = RSTRING_LEN(vkey);
        key.mv_data = RSTRING_PTR(vkey);
        value.mv_size = RSTRING_LEN(vval);
        value.mv_data = RSTRING_PTR(vval);

        check(mdb_put(environment_need_txn(database->env), database->dbi, &key, &value, NIL_P(vflags) ? 0 : NUM2INT(vflags)));
        return Qnil;
}

static VALUE database_delete(int argc, VALUE *argv, VALUE self) {
        DATABASE(self, database);
        if (!environment_get_txn(database->env))
                return call_with_transaction(database->env, self, "delete", argc, argv, 0);

        VALUE vkey, vval;
        rb_scan_args(argc, argv, "11", &vkey, &vval);

        vkey = StringValue(vkey);

        MDB_val key;
        key.mv_size = RSTRING_LEN(vkey);
        key.mv_data = RSTRING_PTR(vkey);

        if (NIL_P(vval)) {
                check(mdb_del(environment_need_txn(database->env), database->dbi, &key, 0));
        } else {
                VALUE vval = StringValue(vval);
                MDB_val value;
                value.mv_size = RSTRING_LEN(vval);
                value.mv_data = RSTRING_PTR(vval);
                check(mdb_del(environment_need_txn(database->env), database->dbi, &key, &value));
        }

        return Qnil;
}

static void cursor_free(Cursor* cursor) {
        if (cursor->cur)
                mdb_cursor_close(cursor->cur);

        database_deref((Database*)DATA_PTR(cursor->db));
        free(cursor);
}

static void cursor_check(Cursor* cursor) {
        if (!cursor->cur)
                rb_raise(cError, "Cursor is closed");
}

static void cursor_mark(Cursor* cursor) {
        rb_gc_mark(cursor->db);
}

static VALUE cursor_close(VALUE self) {
        CURSOR(self, cursor);
        mdb_cursor_close(cursor->cur);
        cursor->cur = 0;
        return Qnil;
}

static VALUE database_cursor(VALUE self) {
        DATABASE(self, database);
        if (!environment_get_txn(database->env))
                return call_with_transaction(database->env, self, "cursor", 0, 0, 0);

        MDB_cursor* cur;
        check(mdb_cursor_open(environment_need_txn(database->env), database->dbi, &cur));

        Cursor* cursor;
        VALUE vcur = Data_Make_Struct(cCursor, Cursor, cursor_mark, cursor_free, cursor);
        cursor->cur = cur;
        cursor->db = self;
        ++database->refcount;

        if (rb_block_given_p()) {
                int exception;
                VALUE result = rb_protect(rb_yield, vcur, &exception);
                if (exception) {
                        cursor_close(vcur);
                        rb_jump_tag(exception);
                }
                cursor_close(vcur);
                return result;
        }

        return vcur;
}

static VALUE cursor_first(VALUE self) {
        CURSOR(self, cursor);
        MDB_val key, value;

        check(mdb_cursor_get(cursor->cur, &key, &value, MDB_FIRST));
        return rb_assoc_new(rb_str_new(key.mv_data, key.mv_size), rb_str_new(value.mv_data, value.mv_size));
}

static VALUE cursor_prev(VALUE self) {
        CURSOR(self, cursor);
        MDB_val key, value;

        check(mdb_cursor_get(cursor->cur, &key, &value, MDB_PREV));
        return rb_assoc_new(rb_str_new(key.mv_data, key.mv_size), rb_str_new(value.mv_data, value.mv_size));
}

static VALUE cursor_next(VALUE self) {
        CURSOR(self, cursor);
        MDB_val key, value;

        check(mdb_cursor_get(cursor->cur, &key, &value, MDB_NEXT));
        return rb_assoc_new(rb_str_new(key.mv_data, key.mv_size), rb_str_new(value.mv_data, value.mv_size));
}

static VALUE cursor_set(VALUE self, VALUE vkey) {
        CURSOR(self, cursor);
        MDB_val key, value;

        key.mv_size = RSTRING_LEN(vkey);
        key.mv_data = StringValuePtr(vkey);

        check(mdb_cursor_get(cursor->cur, &key, &value, MDB_SET_KEY));
        return rb_assoc_new(rb_str_new(key.mv_data, key.mv_size), rb_str_new(value.mv_data, value.mv_size));
}

static VALUE cursor_set_range(VALUE self, VALUE vkey) {
        CURSOR(self, cursor);
        MDB_val key, value;

        key.mv_size = RSTRING_LEN(vkey);
        key.mv_data = StringValuePtr(vkey);

        check(mdb_cursor_get(cursor->cur, &key, &value, MDB_SET_RANGE));
        return rb_assoc_new(rb_str_new(key.mv_data, key.mv_size), rb_str_new(value.mv_data, value.mv_size));
}

static VALUE cursor_get(VALUE self) {
        CURSOR(self, cursor);

        MDB_val key, value;
        int ret = mdb_cursor_get(cursor->cur, &key, &value, MDB_GET_CURRENT);
        if (ret == MDB_NOTFOUND)
                return Qnil;
        check(ret);
        return rb_assoc_new(rb_str_new(key.mv_data, key.mv_size), rb_str_new(value.mv_data, value.mv_size));
}

static VALUE cursor_put(int argc, VALUE* argv, VALUE self) {
        CURSOR(self, cursor);

        VALUE vkey, vflags, vval;
        rb_scan_args(argc, argv, "21", &vkey, &vval, &vflags);

        vkey = StringValue(vkey);
        vval = StringValue(vval);

        MDB_val key, value;
        key.mv_size = RSTRING_LEN(vkey);
        key.mv_data = RSTRING_PTR(vkey);
        value.mv_size = RSTRING_LEN(vval);
        value.mv_data = RSTRING_PTR(vval);

        check(mdb_cursor_put(cursor->cur, &key, &value, NIL_P(vflags) ? 0 : NUM2INT(vflags)));
        return Qnil;
}

static VALUE cursor_delete(int argc, VALUE *argv, VALUE self) {
        CURSOR(self, cursor);
        VALUE vflags;
        rb_scan_args(argc, argv, "01", &vflags);
        check(mdb_cursor_del(cursor->cur, NIL_P(vflags) ? 0 : NUM2INT(vflags)));
        return Qnil;
}

static VALUE cursor_count(VALUE self) {
        CURSOR(self, cursor);
        size_t count;
        check(mdb_cursor_count(cursor->cur, &count));
        return INT2NUM(count);
}

void Init_lmdb_ext() {
        VALUE mLMDB;

        mLMDB = rb_define_module("LMDB");
        rb_define_const(mLMDB, "VERSION", rb_str_new2(MDB_VERSION_STRING));
        rb_define_singleton_method(mLMDB, "open", environment_open, -1);

#define NUM_CONST(name) rb_define_const(mLMDB, #name, INT2NUM(MDB_##name))

        // Versions
        NUM_CONST(VERSION_MAJOR);
        NUM_CONST(VERSION_MINOR);
        NUM_CONST(VERSION_PATCH);

        // Environment flags
        NUM_CONST(FIXEDMAP);
        NUM_CONST(NOSUBDIR);
        NUM_CONST(NOSYNC);
        NUM_CONST(RDONLY);
        NUM_CONST(NOMETASYNC);
        NUM_CONST(WRITEMAP);
        NUM_CONST(MAPASYNC);
        NUM_CONST(NOTLS);

        // Database flags
        NUM_CONST(REVERSEKEY);
        NUM_CONST(DUPSORT);
        NUM_CONST(INTEGERKEY);
        NUM_CONST(DUPFIXED);
        NUM_CONST(INTEGERDUP);
        NUM_CONST(REVERSEDUP);
        NUM_CONST(CREATE);

        // Write flags
        NUM_CONST(NOOVERWRITE);
        NUM_CONST(NODUPDATA);
        NUM_CONST(CURRENT);
        NUM_CONST(RESERVE);
        NUM_CONST(APPEND);
        NUM_CONST(APPENDDUP);
        NUM_CONST(MULTIPLE);

        cError = rb_define_class_under(mLMDB, "Error", rb_eRuntimeError);
#define ERROR(name) cError_##name = rb_define_class_under(cError, #name, cError);
#include "errors.h"
#undef ERROR

        cEnvironment = rb_define_class_under(mLMDB, "Environment", rb_cObject);
        rb_undef_method(rb_singleton_class(cEnvironment), "new");
        rb_define_singleton_method(cEnvironment, "open", environment_open, -1);
        rb_define_method(cEnvironment, "database", environment_database, -1);
        rb_define_method(cEnvironment, "close", environment_close, 0);
        rb_define_method(cEnvironment, "stat", environment_stat, 0);
        rb_define_method(cEnvironment, "info", environment_info, 0);
        rb_define_method(cEnvironment, "copy", environment_copy, 1);
        rb_define_method(cEnvironment, "sync", environment_sync, -1);
        rb_define_method(cEnvironment, "flags=", environment_set_flags, 1);
        rb_define_method(cEnvironment, "flags", environment_flags, 0);
        rb_define_method(cEnvironment, "path", environment_path, 0);
        rb_define_method(cEnvironment, "transaction", environment_transaction, -1);

        cDatabase = rb_define_class_under(mLMDB, "Database", rb_cObject);
        rb_undef_method(rb_singleton_class(cDatabase), "new");
        rb_define_method(cDatabase, "stat", database_stat, 0);
        rb_define_method(cDatabase, "drop", database_drop, 0);
        rb_define_method(cDatabase, "clear", database_clear, 0);
        rb_define_method(cDatabase, "get", database_get, 1);
        rb_define_method(cDatabase, "put", database_put, -1);
        rb_define_method(cDatabase, "delete", database_delete, -1);
        rb_define_method(cDatabase, "cursor", database_cursor, 0);

        cTransaction = rb_define_class_under(mLMDB, "Transaction", rb_cObject);
        rb_undef_method(rb_singleton_class(cCursor), "new");
        rb_define_method(cTransaction, "commit", transaction_commit, 0);
        rb_define_method(cTransaction, "abort", transaction_abort, 0);

        cCursor = rb_define_class_under(mLMDB, "Cursor", rb_cObject);
        rb_undef_method(rb_singleton_class(cCursor), "new");
        rb_define_method(cCursor, "close", cursor_close, 0);
        rb_define_method(cCursor, "get", cursor_get, 0);
        rb_define_method(cCursor, "first", cursor_first, 0);
        rb_define_method(cCursor, "next", cursor_next, 0);
        rb_define_method(cCursor, "prev", cursor_prev, 0);
        rb_define_method(cCursor, "set", cursor_set, 1);
        rb_define_method(cCursor, "set_range", cursor_set_range, 1);
        rb_define_method(cCursor, "put", cursor_put, 0);
        rb_define_method(cCursor, "count", cursor_count, 0);
        rb_define_method(cCursor, "delete", cursor_delete, 0);
}
