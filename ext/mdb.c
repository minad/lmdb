#include "ruby.h"
#include "lmdb.h"

#define ENV_FLAGS (MDB_FIXEDMAP |               \
                   MDB_NOSUBDIR           |     \
                   MDB_NOSYNC             |     \
                   MDB_RDONLY             |     \
                   MDB_NOMETASYNC         |     \
                   MDB_WRITEMAP           |     \
                   MDB_MAPASYNC)

static VALUE cError, cEnvironment, cStat, cInfo, cDatabase, cTransaction, cCursor;

#define ERROR(name) static VALUE cError_##name;
#include "errors.h"
#undef ERROR

#define ENVIRONMENT(var, var_environment)                               \
        Environment* var_environment;                                   \
        Data_Get_Struct(var, Environment, var_environment);             \
        do { if (!var_environment->env) rb_raise(cError, "Environment is closed"); } while(0)

#define TRANSACTION(var, var_transaction, var_environment)              \
        Transaction* var_transaction;                                   \
        Data_Get_Struct(var, Transaction, var_transaction);             \
        {                                                               \
                Transaction* parent = var_transaction;                  \
                for (;;) {                                              \
                        if (!parent->txn)                               \
                                rb_raise(cError, "Transaction is terminated"); \
                        if (NIL_P(parent->parent))                      \
                                break;                                  \
                        Data_Get_Struct(parent->parent, Transaction, parent); \
                }                                                       \
        }                                                               \
        ENVIRONMENT(transaction->environment, var_environment)

#define DATABASE(var, var_database, var_environment)                    \
        Database* var_database;                                         \
        Data_Get_Struct(var, Database, var_database);                       \
        if (!var_database->open) rb_raise(cError, "Database is closed"); \
        ENVIRONMENT(database->environment, var_environment)

#define DATABASE_TRANSACTION(dbvar, txvar, var_database, var_transaction, var_environment) \
        DATABASE(dbvar, var_database, tmp_environment);   \
        TRANSACTION(txvar, var_transaction, var_environment);           \
        do { if (var_environment != tmp_environment) rb_raise(cError, "Different environments"); } while(0)

#define CURSOR(var, var_cursor, var_database, var_transaction, var_environment)    \
        Cursor* var_cursor;                                            \
        Data_Get_Struct(var, Cursor, var_cursor);                           \
        if (!cursor->cur) rb_raise(cError, "Cursor is closed"); \
        DATABASE_TRANSACTION(var_cursor->database, var_cursor->transaction, var_database, var_transaction, var_environment)

typedef struct {
        MDB_env* env;
} Environment;

typedef struct {
        VALUE   environment;
        MDB_dbi dbi;
        int     open;
} Database;

typedef struct {
        VALUE    environment;
        VALUE    parent;
        MDB_txn* txn;
} Transaction;

typedef struct {
        VALUE       transaction;
        VALUE       database;
        MDB_cursor* cur;
} Cursor;

#define STAT(name) \
static VALUE stat_##name(VALUE self) { \
        MDB_stat* stat;                         \
        Data_Get_Struct(self, MDB_stat, stat);  \
        return INT2NUM(stat->ms_##name);       \
}

STAT(psize)
STAT(depth)
STAT(branch_pages)
STAT(leaf_pages)
STAT(overflow_pages)
STAT(entries)

#define INFO(name) \
static VALUE info_##name(VALUE self) { \
        MDB_envinfo* info;                              \
        Data_Get_Struct(self, MDB_envinfo, info);       \
        return INT2NUM((size_t)info->me_##name);        \
}

INFO(mapaddr)
INFO(mapsize)
INFO(last_pgno)
INFO(last_txnid)
INFO(maxreaders)
INFO(numreaders)

static void check(int code) {
        if (!code)
                return;
        const char *error = mdb_strerror(code);
        const char* s = strchr(error, ':');
        if (s)
                error = s + 2;
#define ERROR(name) if (code == MDB_##name) rb_raise(cError_##name, error);
#include "errors.h"
#undef ERROR
        rb_raise(cError, error);
}

static void environment_free(Environment *environment) {
        if (environment->env)
                mdb_env_close(environment->env);
        free(environment);
}

static VALUE environment_close(VALUE self) {
        ENVIRONMENT(self, environment);
        mdb_env_close(environment->env);
        environment->env = 0;
        return Qnil;
}

static VALUE environment_stat(VALUE self) {
        ENVIRONMENT(self, environment);
        MDB_stat* stat;
        VALUE vstat = Data_Make_Struct(cStat, MDB_stat, 0, -1, stat);
        check(mdb_env_stat(environment->env, stat));
        return vstat;
}

static VALUE environment_info(VALUE self) {
        ENVIRONMENT(self, environment);
        MDB_envinfo* info;
        VALUE vinfo = Data_Make_Struct(cInfo, MDB_envinfo, 0, -1, info);
        check(mdb_env_info(environment->env, info));
        return vinfo;
}

static VALUE environment_copy(VALUE self, VALUE path) {
        ENVIRONMENT(self, environment);
        check(mdb_env_copy(environment->env, StringValueCStr(path)));
        return Qnil;
}

static VALUE environment_sync(int argc, VALUE *argv, VALUE self) {
        ENVIRONMENT(self, environment);
        VALUE force;
        int n = rb_scan_args(argc, argv, "01", &force);
        check(mdb_env_sync(environment->env, n == 1 && RTEST(force) ? 0 : 1));
        return Qnil;
}

static VALUE environment_open(int argc, VALUE *argv, VALUE klass) {
        VALUE path, options;
        int n = rb_scan_args(argc, argv, "11", &path, &options);

        int flags = 0, maxreaders = -1, mapsize = -1, maxdbs = 10;
        mode_t mode = 0755;
        if (n == 2) {
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
                        mapsize = NUM2INT(value);
        }

        MDB_env* env;
        check(mdb_env_create(&env));

        Environment* environment;
        VALUE venv = Data_Make_Struct(cEnvironment, Environment, 0, environment_free, environment);
        environment->env = env;

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
        ENVIRONMENT(self, environment);
        int flags;
        check(mdb_env_get_flags(environment->env, &flags));
        return INT2NUM(flags & ENV_FLAGS);
}

static VALUE environment_path(VALUE self) {
        ENVIRONMENT(self, environment);
        const char* path;
        check(mdb_env_get_path(environment->env, &path));
        return rb_str_new2(path);
}

static VALUE environment_set_flags(VALUE self, VALUE vflags) {
        ENVIRONMENT(self, environment);
        int flags = NUM2INT(vflags), oldflags;
        check(mdb_env_get_flags(environment->env, &oldflags));
        check(mdb_env_set_flags(environment->env, oldflags & ENV_FLAGS, 0));
        check(mdb_env_set_flags(environment->env, flags, 1));
        return environment_flags(self);
}

static VALUE transaction_environment(VALUE self) {
        TRANSACTION(self, transaction, environment);
        return transaction->environment;
}

static VALUE transaction_parent(VALUE self) {
        TRANSACTION(self, transaction, environment);
        return transaction->parent;
}

static void transaction_mark(Transaction* transaction) {
        rb_gc_mark(transaction->environment);
        rb_gc_mark(transaction->parent);
}

static void transaction_free(Transaction *transaction) {
        if (transaction->txn)
                mdb_txn_abort(transaction->txn);
        free(transaction);
}

VALUE transaction_abort(VALUE self) {
        TRANSACTION(self, transaction, environment);
        mdb_txn_abort(transaction->txn);
        transaction->txn = 0;
        return Qnil;
}

VALUE transaction_commit(VALUE self) {
        TRANSACTION(self, transaction, environment);
        mdb_txn_commit(transaction->txn);
        transaction->txn = 0;
        return Qnil;
}

VALUE transaction_renew(VALUE self) {
        TRANSACTION(self, transaction, environment);
        mdb_txn_renew(transaction->txn);
        return Qnil;
}

VALUE transaction_reset(VALUE self) {
        TRANSACTION(self, transaction, environment);
        mdb_txn_reset(transaction->txn);
        return Qnil;
}

static VALUE environment_transaction(int argc, VALUE *argv, VALUE self) {
        ENVIRONMENT(self, environment);

        VALUE readonly;
        int flags = (rb_scan_args(argc, argv, "01", &readonly) == 1 && !NIL_P(readonly)) ? MDB_RDONLY : 0;

        MDB_txn* txn;
        check(mdb_txn_begin(environment->env, 0, flags, &txn));

        Transaction* transaction;
        VALUE vtxn = Data_Make_Struct(cTransaction, Transaction, transaction_mark, transaction_free, transaction);
        transaction->txn = txn;
        transaction->environment = self;
        transaction->parent = Qnil;

        if (rb_block_given_p()) {
                int exception;
                VALUE result = rb_protect(rb_yield, vtxn, &exception);
                if (exception) {
                        mdb_txn_abort(transaction->txn);
                        transaction->txn = 0;
                        rb_jump_tag(exception);
                }
                mdb_txn_commit(transaction->txn);
                transaction->txn = 0;
                return result;
        }
        return vtxn;
}

static VALUE transaction_transaction(VALUE self) {
        TRANSACTION(self, transaction, environment);

        MDB_txn* txn;
        check(mdb_txn_begin(environment->env, transaction->txn, 0, &txn));

        Transaction* child;
        VALUE vtxn = Data_Make_Struct(cTransaction, Transaction, transaction_mark, transaction_free, child);
        child->txn = txn;
        child->environment = transaction->environment;
        child->parent = self;

        if (rb_block_given_p()) {
                int exception;
                VALUE result = rb_protect(rb_yield, vtxn, &exception);
                if (exception) {
                        mdb_txn_abort(child->txn);
                        child->txn = 0;
                        rb_jump_tag(exception);
                }
                mdb_txn_commit(child->txn);
                child->txn = 0;
                return result;
        }
        return vtxn;
}

static VALUE database_environment(VALUE self) {
        DATABASE(self, database, environment);
        return database->environment;
}

static void database_free(Database* database) {
        if (database->open) {
                ENVIRONMENT(database->environment, environment);
                mdb_dbi_close(environment->env, database->dbi);
        }
        free(database);
}

static void database_mark(Database* database) {
        rb_gc_mark(database->environment);
}

static VALUE database_open(int argc, VALUE *argv, VALUE self) {
        ENVIRONMENT(self, environment);

        VALUE vtxn, vname, vflags;
        int n = rb_scan_args(argc, argv, "21", &vtxn, &vname, &vflags);

        TRANSACTION(vtxn, transaction, txn_environment);
        if (environment != txn_environment)
                rb_raise(cError, "Different environments");

        MDB_dbi dbi;
        check(mdb_dbi_open(transaction->txn, StringValueCStr(vname), n == 3 ? NUM2INT(vflags) : 0, &dbi));

        Database* database;
        VALUE vdb = Data_Make_Struct(cDatabase, Database, database_mark, database_free, database);
        database->dbi = dbi;
        database->environment = self;
        database->open = 1;

        return vdb;
}

static VALUE database_close(VALUE self) {
        DATABASE(self, database, environment);
        mdb_dbi_close(environment->env, database->dbi);
        database->open = 0;
        return Qnil;
}

static VALUE database_stat(VALUE self, VALUE vtxn) {
        DATABASE_TRANSACTION(self, vtxn, database, transaction, environment);
        MDB_stat* stat;
        VALUE vstat = Data_Make_Struct(cStat, MDB_stat, 0, -1, stat);
        check(mdb_stat(transaction->txn, database->dbi, stat));
        return vstat;
}

static VALUE database_drop(VALUE self, VALUE vtxn) {
        DATABASE_TRANSACTION(self, vtxn, database, transaction, environment);
        check(mdb_drop(transaction->txn, database->dbi, 1));
        database->open = 0;
        return Qnil;
}

static VALUE database_clear(VALUE self, VALUE vtxn) {
        DATABASE_TRANSACTION(self, vtxn, database, transaction, environment);
        check(mdb_drop(transaction->txn, database->dbi, 0));
        return Qnil;
}

static VALUE database_get(VALUE self, VALUE vtxn, VALUE vkey) {
        DATABASE_TRANSACTION(self, vtxn, database, transaction, environment);
        vkey = StringValue(vkey);
        MDB_val key, value;
        key.mv_size = RSTRING_LEN(vkey);
        key.mv_data = RSTRING_PTR(vkey);
        check(mdb_get(transaction->txn, database->dbi, &key, &value));
        return rb_str_new(value.mv_data, value.mv_size);
}

static VALUE database_put(int argc, VALUE *argv, VALUE self) {
        VALUE vtxn, vkey, vval, vflags;
        int n = rb_scan_args(argc, argv, "31", &vtxn, &vkey, &vval, &vflags);

        DATABASE_TRANSACTION(self, vtxn, database, transaction, environment);

        vkey = StringValue(vkey);
        vval = StringValue(vval);

        MDB_val key, value;
        key.mv_size = RSTRING_LEN(vkey);
        key.mv_data = RSTRING_PTR(vkey);
        value.mv_size = RSTRING_LEN(vval);
        value.mv_data = RSTRING_PTR(vval);

        check(mdb_put(transaction->txn, database->dbi, &key, &value, n == 4 ? NUM2INT(vflags) : 0));
        return Qnil;
}

static VALUE database_delete(int argc, VALUE *argv, VALUE self) {
        VALUE vtxn, vkey, vval;
        int n = rb_scan_args(argc, argv, "21", &vtxn, &vkey, &vval);

        DATABASE_TRANSACTION(self, vtxn, database, transaction, environment);

        vkey = StringValue(vkey);

        MDB_val key;
        key.mv_size = RSTRING_LEN(vkey);
        key.mv_data = RSTRING_PTR(vkey);

        if (n == 3) {
                vval = StringValue(vval);
                MDB_val value;
                value.mv_size = RSTRING_LEN(vval);
                value.mv_data = RSTRING_PTR(vval);
                check(mdb_del(transaction->txn, database->dbi, &key, &value));
        } else {
                check(mdb_del(transaction->txn, database->dbi, &key, 0));
        }

        return Qnil;
}

static void cursor_free(Cursor* cursor) {
        if (cursor->cur)
                mdb_cursor_close(cursor->cur);
        free(cursor);
}

static void cursor_mark(Cursor* cursor) {
        rb_gc_mark(cursor->database);
        rb_gc_mark(cursor->transaction);
}

static VALUE database_cursor(VALUE self, VALUE vtxn) {
        DATABASE_TRANSACTION(self, vtxn, database, transaction, environment);

        MDB_cursor* cur;
        check(mdb_cursor_open(transaction->txn, database->dbi, &cur));

        Cursor* cursor;
        VALUE vcur = Data_Make_Struct(cCursor, Cursor, cursor_mark, cursor_free, cursor);
        cursor->cur = cur;
        cursor->database = self;
        cursor->transaction = vtxn;

        return vcur;
}

static VALUE cursor_transaction(VALUE self) {
        CURSOR(self, cursor, database, transaction, environment);
        return cursor->transaction;
}

static VALUE cursor_database(VALUE self) {
        CURSOR(self, cursor, database, transaction, environment);
        return cursor->database;
}

static VALUE cursor_close(VALUE self) {
        CURSOR(self, cursor, database, transaction, environment);
        mdb_cursor_close(cursor->cur);
        cursor->cur = 0;
        return Qnil;
}

static VALUE cursor_get(VALUE self) {
        CURSOR(self, cursor, database, transaction, environment);
        // TODO
        return Qnil;
}

static VALUE cursor_put(VALUE self) {
        CURSOR(self, cursor, database, transaction, environment);
        // TODO
        return Qnil;
}

static VALUE cursor_delete(int argc, VALUE *argv, VALUE self) {
        CURSOR(self, cursor, database, transaction, environment);
        VALUE flags;
        int n = rb_scan_args(argc, argv, "01", &flags);
        check(mdb_cursor_del(cursor->cur, n == 1 ? NUM2INT(flags) : 0));
        return Qnil;
}

static VALUE cursor_count(VALUE self) {
        CURSOR(self, cursor, database, transaction, environment);
        size_t count;
        check(mdb_cursor_count(cursor->cur, &count));
        return INT2NUM(count);
}

void Init_mdb() {
	VALUE mMDB = rb_define_module("MDB");
        rb_define_singleton_method(mMDB, "open", environment_open, -1);
        rb_define_const(mMDB, "VERSION", rb_str_new2(MDB_VERSION_STRING));

#define NUM_CONST(name) rb_define_const(mMDB, #name, INT2NUM(MDB_##name))

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

        // Database flags
        NUM_CONST(REVERSEKEY);
        NUM_CONST(DUPSORT);
        NUM_CONST(INTEGERKEY);
        NUM_CONST(DUPFIXED);
        NUM_CONST(INTEGERDUP);
        NUM_CONST(REVERSEDUP);
        NUM_CONST(CREATE);
        NUM_CONST(NOOVERWRITE);
        NUM_CONST(NODUPDATA);
        NUM_CONST(CURRENT);
        NUM_CONST(RESERVE);
        NUM_CONST(APPEND);
        NUM_CONST(APPENDDUP);
        NUM_CONST(MULTIPLE);

        cError = rb_define_class_under(mMDB, "Error", rb_eRuntimeError);

#define ERROR(name) cError_##name = rb_define_class_under(cError, #name, cError);
#include "errors.h"
#undef ERROR

        cStat = rb_define_class_under(mMDB, "Stat", rb_cObject);
        rb_define_method(cStat, "psize", stat_psize, 0);
        rb_define_method(cStat, "depth", stat_depth, 0);
        rb_define_method(cStat, "branch_pages", stat_branch_pages, 0);
        rb_define_method(cStat, "leaf_pages", stat_leaf_pages, 0);
        rb_define_method(cStat, "overflow_pages", stat_overflow_pages, 0);
        rb_define_method(cStat, "entries", stat_entries, 0);

        cInfo = rb_define_class_under(mMDB, "Info", rb_cObject);
        rb_define_method(cInfo, "mapaddr", info_mapaddr, 0);
        rb_define_method(cInfo, "mapsize", info_mapsize, 0);
        rb_define_method(cInfo, "last_pgno", info_last_pgno, 0);
        rb_define_method(cInfo, "last_txnid", info_last_txnid, 0);
        rb_define_method(cInfo, "maxreaders", info_maxreaders, 0);
        rb_define_method(cInfo, "numreaders", info_numreaders, 0);

	cEnvironment = rb_define_class_under(mMDB, "Environment", rb_cObject);
        rb_define_singleton_method(cEnvironment, "open", environment_open, -1);
        rb_define_method(cEnvironment, "close", environment_close, 0);
        rb_define_method(cEnvironment, "stat", environment_stat, 0);
        rb_define_method(cEnvironment, "info", environment_info, 0);
        rb_define_method(cEnvironment, "copy", environment_copy, 1);
        rb_define_method(cEnvironment, "sync", environment_sync, -1);
        rb_define_method(cEnvironment, "flags=", environment_set_flags, 1);
        rb_define_method(cEnvironment, "flags", environment_flags, 0);
        rb_define_method(cEnvironment, "path", environment_path, 0);
        rb_define_method(cEnvironment, "transaction", environment_transaction, -1);
        rb_define_method(cEnvironment, "open", database_open, -1);

        cDatabase = rb_define_class_under(mMDB, "Database", rb_cObject);
        rb_define_method(cDatabase, "close", database_close, 0);
        rb_define_method(cDatabase, "stat", database_stat, 0);
        rb_define_method(cDatabase, "drop", database_drop, 1);
        rb_define_method(cDatabase, "clear", database_clear, 1);
        rb_define_method(cDatabase, "get", database_get, 2);
        rb_define_method(cDatabase, "put", database_put, -1);
        rb_define_method(cDatabase, "delete", database_delete, -1);
        rb_define_method(cDatabase, "cursor", database_cursor, 0);
        rb_define_method(cDatabase, "environment", database_environment, 0);

        cTransaction = rb_define_class_under(mMDB, "Transaction", rb_cObject);
        rb_define_method(cTransaction, "abort", transaction_abort, 0);
        rb_define_method(cTransaction, "commit", transaction_commit, 0);
        rb_define_method(cTransaction, "reset", transaction_reset, 0);
        rb_define_method(cTransaction, "renew", transaction_renew, 0);
        rb_define_method(cTransaction, "transaction", transaction_transaction, 0);
        rb_define_method(cTransaction, "environment", transaction_environment, 0);
        rb_define_method(cTransaction, "parent", transaction_parent, 0);

        cCursor = rb_define_class_under(mMDB, "Cursor", rb_cObject);
        rb_define_method(cCursor, "close", cursor_close, 0);
        rb_define_method(cCursor, "get", cursor_get, 0);
        rb_define_method(cCursor, "put", cursor_put, 0);
        rb_define_method(cCursor, "delete", cursor_delete, 0);
        rb_define_method(cCursor, "database", cursor_database, 0);
        rb_define_method(cCursor, "transaction", cursor_transaction, 0);
}
