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
                if (transaction->txn) {
                        rb_warn("Garbage collecting active transaction!");
                        mdb_txn_abort(transaction->txn);
                }
                free(transaction);
        }
}

static void transaction_mark(Transaction* transaction) {
        rb_gc_mark(transaction->parent);
        rb_gc_mark(transaction->env);
}

static VALUE transaction_commit(VALUE self) {
        transaction_finish(self, 1);
        return Qnil;
}

static VALUE transaction_abort(VALUE self) {
        transaction_finish(self, 0);
        return Qnil;
}

static void transaction_finish(VALUE self, int commit) {
        TRANSACTION(self, transaction);

        if (!transaction->txn)
                rb_raise(cError, "Transaction is terminated");

        if (transaction->thread != rb_thread_current())
                rb_raise(cError, "Wrong thread");

        // Check nesting
        VALUE p = environment_active_txn(transaction->env);
        while (!NIL_P(p) && p != self) {
                TRANSACTION(p, txn);
                p = txn->parent;
        }
        if (p != self)
                rb_raise(cError, "Transaction is not active");

        int ret = 0;
        if (commit)
                ret = mdb_txn_commit(transaction->txn);
        else
                mdb_txn_abort(transaction->txn);

        p = environment_active_txn(transaction->env);
        while (p != self) {
                TRANSACTION(p, txn);
                txn->txn = 0;
                p = txn->parent;
        }
        transaction->txn = 0;

        environment_set_active_txn(transaction->env, transaction->thread, transaction->parent);

        check(ret);
}

// Ruby 1.8.7 compatibility
#ifndef HAVE_RB_FUNCALL_PASSING_BLOCK
static VALUE call_with_transaction_helper2(VALUE arg) {
        HelperArgs* a = (HelperArgs*)arg;
        return rb_funcall_passing_block(a->self, rb_intern(a->name), a->argc, a->argv);
}

static VALUE call_with_transaction_helper(VALUE arg) {
        return rb_block_pass(call_with_transaction_helper2, arg, rb_block_proc());
}
#else
static VALUE call_with_transaction_helper(VALUE arg) {
        HelperArgs* a = (HelperArgs*)arg;
        return rb_funcall_passing_block(a->self, rb_intern(a->name), a->argc, a->argv);
}
#endif

static VALUE call_with_transaction(VALUE venv, VALUE self, const char* name, int argc, const VALUE* argv, int flags) {
        HelperArgs arg = { self, name, argc, argv };
        return with_transaction(venv, call_with_transaction_helper, (VALUE)&arg, flags);
}

static VALUE with_transaction(VALUE venv, VALUE(*fn)(VALUE), VALUE arg, int flags) {
        ENVIRONMENT(venv, environment);

        MDB_txn* txn;
        check(mdb_txn_begin(environment->env, active_txn(venv), flags, &txn));

        Transaction* transaction;
        VALUE vtxn = Data_Make_Struct(cTransaction, Transaction, transaction_mark, transaction_deref, transaction);
        transaction->refcount = 1;
        transaction->parent = environment_active_txn(venv);
        transaction->env = venv;
        transaction->txn = txn;
        transaction->thread = rb_thread_current();
        environment_set_active_txn(venv, transaction->thread, vtxn);

        if (!NIL_P(transaction->parent)) {
                TRANSACTION(transaction->parent, parent);
                ++parent->refcount;
        }

        ++environment->refcount;

        int exception;
        VALUE ret = rb_protect(fn, NIL_P(arg) ? vtxn : arg, &exception);

        if (exception) {
                if (vtxn == environment_active_txn(venv))
                        transaction_abort(vtxn);
                rb_jump_tag(exception);
        }
        if (vtxn == environment_active_txn(venv))
                transaction_commit(vtxn);
        return ret;
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


static void environment_mark(Environment* environment) {
        rb_gc_mark(environment->thread_txn_hash);
        rb_gc_mark(environment->txn_thread_hash);
}

static VALUE environment_close(VALUE self) {
        ENVIRONMENT(self, environment);
        mdb_env_close(environment->env);
        environment->env = 0;
        return Qnil;
}

static VALUE stat2hash(const MDB_stat* stat) {
        VALUE ret = rb_hash_new();

#define STAT_SET(name) rb_hash_aset(ret, ID2SYM(rb_intern(#name)), INT2NUM(stat->ms_##name))
        STAT_SET(psize);
        STAT_SET(depth);
        STAT_SET(branch_pages);
        STAT_SET(leaf_pages);
        STAT_SET(overflow_pages);
        STAT_SET(entries);
#undef STAT_SET

        return ret;
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

        VALUE ret = rb_hash_new();

#define INFO_SET(name) rb_hash_aset(ret, ID2SYM(rb_intern(#name)), INT2NUM((size_t)info.me_##name));
        INFO_SET(mapaddr);
        INFO_SET(mapsize);
        INFO_SET(last_pgno);
        INFO_SET(last_txnid);
        INFO_SET(maxreaders);
        INFO_SET(numreaders);
#undef INFO_SET

        return ret;
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

static int environment_options(VALUE key, VALUE value, EnvironmentOptions* options) {
        ID id = rb_to_id(key);

        if (id == rb_intern("mode"))
                options->mode = NUM2INT(value);
        else if (id == rb_intern("maxreaders"))
                options->maxreaders = NUM2INT(value);
        else if (id == rb_intern("maxdbs"))
                options->maxdbs = NUM2INT(value);
        else if (id == rb_intern("mapsize"))
                options->mapsize = NUM2SSIZET(value);

#define FLAG(const, name) else if (id == rb_intern(#name)) { if (RTEST(value)) { options->flags |= MDB_##const; } }
#include "env_flags.h"
#undef FLAG

        else {
                VALUE s = rb_inspect(key);
                rb_raise(cError, "Invalid option %s", StringValueCStr(s));
        }

        return 0;
}

static VALUE environment_new(int argc, VALUE *argv, VALUE klass) {
        VALUE path, option_hash;
        rb_scan_args(argc, argv, "1:", &path, &option_hash);

        EnvironmentOptions options = {
                .flags = MDB_NOTLS,
                .maxreaders = -1,
                .maxdbs = 128,
                .mapsize = 0,
                .mode = 0755,
        };
        if (!NIL_P(option_hash))
                rb_hash_foreach(option_hash, environment_options, (VALUE)&options);

        MDB_env* env;
        check(mdb_env_create(&env));

        Environment* environment;
        VALUE venv = Data_Make_Struct(cEnvironment, Environment, environment_mark, environment_deref, environment);
        environment->env = env;
        environment->refcount = 1;
        environment->thread_txn_hash = rb_hash_new();
        environment->txn_thread_hash = rb_hash_new();

        if (options.maxreaders > 0)
                check(mdb_env_set_maxreaders(env, options.maxreaders));
        if (options.mapsize > 0)
                check(mdb_env_set_mapsize(env, options.mapsize));

        check(mdb_env_set_maxdbs(env, options.maxdbs <= 0 ? 1 : options.maxdbs));
        check(mdb_env_open(env, StringValueCStr(path), options.flags, options.mode));

        if (rb_block_given_p())
                return rb_ensure(rb_yield, venv, environment_close, venv);

        return venv;
}

static VALUE environment_flags(VALUE self) {
        unsigned int flags;
        ENVIRONMENT(self, environment);
        check(mdb_env_get_flags(environment->env, &flags));

        VALUE ret = rb_ary_new();
#define FLAG(const, name) if (flags & MDB_##const) rb_ary_push(ret, ID2SYM(rb_intern(#name)));
#include "env_flags.h"
#undef FLAG

        return ret;
}

static VALUE environment_path(VALUE self) {
        const char* path;
        ENVIRONMENT(self, environment);
        check(mdb_env_get_path(environment->env, &path));
        return rb_str_new2(path);
}

static VALUE environment_change_flags(int argc, VALUE* argv, VALUE self, int set) {
        ENVIRONMENT(self, environment);

        int i;
        for (i = 0; i < argc; ++i) {
                ID id = rb_to_id(argv[i]);

                if (0) {}
#define FLAG(const, name) else if (id == rb_intern(#name)) check(mdb_env_set_flags(environment->env, MDB_##const, set));
#include "env_flags.h"
#undef FLAG
                else
                        rb_raise(cError, "Invalid option %s", StringValueCStr(argv[i]));
        }
        return Qnil;
}

static VALUE environment_set_flags(int argc, VALUE* argv, VALUE self) {
        environment_change_flags(argc, argv, self, 1);
        return Qnil;
}

static VALUE environment_clear_flags(int argc, VALUE* argv, VALUE self) {
        environment_change_flags(argc, argv, self, 0);
        return Qnil;
}

static VALUE environment_active_txn(VALUE self) {
        ENVIRONMENT(self, environment);
        return rb_hash_aref(environment->thread_txn_hash, rb_thread_current());
}

static void environment_set_active_txn(VALUE self, VALUE thread, VALUE txn) {
        ENVIRONMENT(self, environment);

        if (NIL_P(txn)) {
                VALUE oldtxn = rb_hash_aref(environment->thread_txn_hash, thread);
                if (!NIL_P(oldtxn)) {
                        rb_hash_delete(environment->thread_txn_hash, thread);
                        rb_hash_delete(environment->txn_thread_hash, oldtxn);
                }
        } else {
                rb_hash_aset(environment->txn_thread_hash, txn, thread);
                rb_hash_aset(environment->thread_txn_hash, thread, txn);
        }
}


static MDB_txn* active_txn(VALUE self) {
        VALUE vtxn = environment_active_txn(self);
        if (NIL_P(vtxn))
                return 0;
        TRANSACTION(vtxn, transaction);
        if (!transaction->txn)
                rb_raise(cError, "Transaction is terminated");
        if (transaction->thread != rb_thread_current())
                rb_raise(cError, "Wrong thread");
        return transaction->txn;
}

static MDB_txn* need_txn(VALUE self) {
        MDB_txn* txn = active_txn(self);
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

#define METHOD database_flags
#define FILE "dbi_flags.h"
#include "flag_parser.h"
#undef METHOD
#undef FILE

static VALUE environment_database(int argc, VALUE *argv, VALUE self) {
        ENVIRONMENT(self, environment);
        if (!active_txn(self))
                return call_with_transaction(self, self, "database", argc, argv, 0);

        VALUE name, option_hash;
        rb_scan_args(argc, argv, "01:", &name, &option_hash);

        int flags = 0;
        if (!NIL_P(option_hash))
                rb_hash_foreach(option_hash, database_flags, (VALUE)&flags);

        MDB_dbi dbi;
        check(mdb_dbi_open(need_txn(self), NIL_P(name) ? 0 : StringValueCStr(name), flags, &dbi));

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
        if (!active_txn(database->env))
                return call_with_transaction(database->env, self, "stat", 0, 0, MDB_RDONLY);

        MDB_stat stat;
        check(mdb_stat(need_txn(database->env), database->dbi, &stat));
        return stat2hash(&stat);
}

static VALUE database_drop(VALUE self) {
        DATABASE(self, database);
        if (!active_txn(database->env))
                return call_with_transaction(database->env, self, "drop", 0, 0, 0);
        check(mdb_drop(need_txn(database->env), database->dbi, 1));
        return Qnil;
}

static VALUE database_clear(VALUE self) {
        DATABASE(self, database);
        if (!active_txn(database->env))
                return call_with_transaction(database->env, self, "clear", 0, 0, 0);
        check(mdb_drop(need_txn(database->env), database->dbi, 0));
        return Qnil;
}

static VALUE database_get(VALUE self, VALUE vkey) {
        DATABASE(self, database);
        if (!active_txn(database->env))
                return call_with_transaction(database->env, self, "get", 1, &vkey, MDB_RDONLY);

        vkey = StringValue(vkey);
        MDB_val key, value;
        key.mv_size = RSTRING_LEN(vkey);
        key.mv_data = RSTRING_PTR(vkey);

        int ret = mdb_get(need_txn(database->env), database->dbi, &key, &value);
        if (ret == MDB_NOTFOUND)
                return Qnil;
        check(ret);
        return rb_str_new(value.mv_data, value.mv_size);
}

#define METHOD database_put_flags
#define FILE "put_flags.h"
#include "flag_parser.h"
#undef METHOD
#undef FILE

static VALUE database_put(int argc, VALUE *argv, VALUE self) {
        DATABASE(self, database);
        if (!active_txn(database->env))
                return call_with_transaction(database->env, self, "put", argc, argv, 0);

        VALUE vkey, vval, option_hash;
        rb_scan_args(argc, argv, "2:", &vkey, &vval, &option_hash);

        int flags = 0;
        if (!NIL_P(option_hash))
                rb_hash_foreach(option_hash, database_put_flags, (VALUE)&flags);

        vkey = StringValue(vkey);
        vval = StringValue(vval);

        MDB_val key, value;
        key.mv_size = RSTRING_LEN(vkey);
        key.mv_data = RSTRING_PTR(vkey);
        value.mv_size = RSTRING_LEN(vval);
        value.mv_data = RSTRING_PTR(vval);

        check(mdb_put(need_txn(database->env), database->dbi, &key, &value, flags));
        return Qnil;
}

static VALUE database_delete(int argc, VALUE *argv, VALUE self) {
        DATABASE(self, database);
        if (!active_txn(database->env))
                return call_with_transaction(database->env, self, "delete", argc, argv, 0);

        VALUE vkey, vval;
        rb_scan_args(argc, argv, "11", &vkey, &vval);

        vkey = StringValue(vkey);

        MDB_val key;
        key.mv_size = RSTRING_LEN(vkey);
        key.mv_data = RSTRING_PTR(vkey);

        if (NIL_P(vval)) {
                check(mdb_del(need_txn(database->env), database->dbi, &key, 0));
        } else {
                VALUE vval = StringValue(vval);
                MDB_val value;
                value.mv_size = RSTRING_LEN(vval);
                value.mv_data = RSTRING_PTR(vval);
                check(mdb_del(need_txn(database->env), database->dbi, &key, &value));
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
        if (!active_txn(database->env))
                return call_with_transaction(database->env, self, "cursor", 0, 0, 0);

        MDB_cursor* cur;
        check(mdb_cursor_open(need_txn(database->env), database->dbi, &cur));

        Cursor* cursor;
        VALUE vcur = Data_Make_Struct(cCursor, Cursor, cursor_mark, cursor_free, cursor);
        cursor->cur = cur;
        cursor->db = self;
        ++database->refcount;

        if (rb_block_given_p()) {
                int exception;
                VALUE ret = rb_protect(rb_yield, vcur, &exception);
                if (exception) {
                        cursor_close(vcur);
                        rb_jump_tag(exception);
                }
                cursor_close(vcur);
                return ret;
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

        int ret = mdb_cursor_get(cursor->cur, &key, &value, MDB_PREV);
        if (ret == MDB_NOTFOUND)
                return Qnil;
        check(ret);
        return rb_assoc_new(rb_str_new(key.mv_data, key.mv_size), rb_str_new(value.mv_data, value.mv_size));
}

static VALUE cursor_next(VALUE self) {
        CURSOR(self, cursor);
        MDB_val key, value;

        int ret = mdb_cursor_get(cursor->cur, &key, &value, MDB_NEXT);
        if (ret == MDB_NOTFOUND)
                return Qnil;
        check(ret);
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

#define METHOD cursor_put_flags
#define FILE "cursor_put_flags.h"
#include "flag_parser.h"
#undef METHOD
#undef FILE

static VALUE cursor_put(int argc, VALUE* argv, VALUE self) {
        CURSOR(self, cursor);

        VALUE vkey, vval, option_hash;
        rb_scan_args(argc, argv, "2:", &vkey, &vval, &option_hash);

        int flags = 0;
        if (!NIL_P(option_hash))
                rb_hash_foreach(option_hash, cursor_put_flags, (VALUE)&flags);

        vkey = StringValue(vkey);
        vval = StringValue(vval);

        MDB_val key, value;
        key.mv_size = RSTRING_LEN(vkey);
        key.mv_data = RSTRING_PTR(vkey);
        value.mv_size = RSTRING_LEN(vval);
        value.mv_data = RSTRING_PTR(vval);

        check(mdb_cursor_put(cursor->cur, &key, &value, flags));
        return Qnil;
}

#define METHOD cursor_delete_flags
#define FILE "cursor_delete_flags.h"
#include "flag_parser.h"
#undef METHOD
#undef FILE

static VALUE cursor_delete(int argc, VALUE *argv, VALUE self) {
        CURSOR(self, cursor);

        VALUE option_hash;
        rb_scan_args(argc, argv, ":", &option_hash);

        int flags = 0;
        if (!NIL_P(option_hash))
                rb_hash_foreach(option_hash, cursor_delete_flags, (VALUE)&flags);

        check(mdb_cursor_del(cursor->cur, flags));
        return Qnil;
}

static VALUE cursor_count(VALUE self) {
        CURSOR(self, cursor);
        size_t count;
        check(mdb_cursor_count(cursor->cur, &count));
        return SIZET2NUM(count);
}

void Init_lmdb_ext() {
        VALUE mLMDB;

        mLMDB = rb_define_module("LMDB");
        rb_define_const(mLMDB, "LIB_VERSION", rb_str_new2(MDB_VERSION_STRING));
        rb_define_singleton_method(mLMDB, "new", environment_new, -1);

#define VERSION_CONST(name) rb_define_const(mLMDB, "LIB_VERSION_"#name, INT2NUM(MDB_VERSION_##name));
        VERSION_CONST(MAJOR)
        VERSION_CONST(MINOR)
        VERSION_CONST(PATCH)
#undef VERSION_CONST

        cError = rb_define_class_under(mLMDB, "Error", rb_eRuntimeError);
#define ERROR(name) cError_##name = rb_define_class_under(cError, #name, cError);
#include "errors.h"
#undef ERROR

        cEnvironment = rb_define_class_under(mLMDB, "Environment", rb_cObject);
        rb_define_singleton_method(cEnvironment, "new", environment_new, -1);
        rb_define_method(cEnvironment, "database", environment_database, -1);
        rb_define_method(cEnvironment, "active_txn", environment_active_txn, 0);
        rb_define_method(cEnvironment, "close", environment_close, 0);
        rb_define_method(cEnvironment, "stat", environment_stat, 0);
        rb_define_method(cEnvironment, "info", environment_info, 0);
        rb_define_method(cEnvironment, "copy", environment_copy, 1);
        rb_define_method(cEnvironment, "sync", environment_sync, -1);
        rb_define_method(cEnvironment, "set_flags", environment_set_flags, -1);
        rb_define_method(cEnvironment, "clear_flags", environment_clear_flags, -1);
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
