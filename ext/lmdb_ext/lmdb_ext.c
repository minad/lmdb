#include "ruby.h"
#include "lmdb.h"

/*-----------------------------------------------------------------------------
 * Macros
 *----------------------------------------------------------------------------*/

#define ENV_FLAGS (   \
    MDB_FIXEDMAP    | \
    MDB_NOSUBDIR    | \
    MDB_NOSYNC      | \
    MDB_RDONLY      | \
    MDB_NOMETASYNC  | \
    MDB_WRITEMAP    | \
    MDB_MAPASYNC)

#define ENVIRONMENT(var, var_env)                           \
    Environment* var_env;                                   \
    Data_Get_Struct(var, Environment, var_env);             \
    environment_check(var_env)

#define TRANSACTION(var, var_txn, no_children)                          \
    Transaction* var_txn;                                               \
    Data_Get_Struct(var, Transaction, var_txn);                         \
    transaction_check(var_txn, no_children)

#define DATABASE(var, var_db)                                   \
    Database* var_db;                                           \
    Data_Get_Struct(var, Database, var_db);                     \
    database_check(var_db)

#define CURSOR(var, var_cur)                                \
    Cursor* var_cur;                                        \
    Data_Get_Struct(var, Cursor, var_cur);                  \
    cursor_check(var_cur)

#define WITH_TRANSACTION(name, required, optional)                      \
    static VALUE name##_with_transaction(WithTransactionArgs*);         \
    static VALUE name(int argc, VALUE *argv, VALUE self) {              \
        DATABASE(self, database);                                       \
        if (argc < required || argc > required + optional + 1)          \
            rb_raise(rb_eRuntimeError, "Wrong number of arguments");    \
        if (argc < 1 || TYPE(argv[0]) != T_DATA) {                      \
            MDB_txn* txn;                                               \
            Transaction *parent = database->transaction;                \
            while (parent->child)                                       \
                    parent = parent->child;                             \
            check(mdb_txn_begin(database->transaction->environment->env, parent, 0, &txn)); \
            WithTransactionArgs args = { database, txn, argc, argv  };  \
            int exception;                                              \
            VALUE result = rb_protect(RUBY_METHOD_FUNC(name##_with_transaction), (VALUE)&args, &exception); \
            if (exception) {                                            \
                mdb_txn_abort(txn);                                     \
                rb_jump_tag(exception);                                 \
            }                                                           \
            mdb_txn_commit(txn);                                        \
            return result;                                              \
        }                                                               \
        TRANSACTION(argv[0], transaction, 1);                           \
        WithTransactionArgs args = { database, transaction->txn, argc - 1, argv + 1 }; \
        return name##_with_transaction(&args);                          \
    }                                                                   \
    static VALUE name##_with_transaction(WithTransactionArgs* a)

/*-----------------------------------------------------------------------------
 * Static
 *----------------------------------------------------------------------------*/

/* Classes */
static VALUE cEnvironment, cStat, cInfo, cDatabase, cTransaction, cCursor, cError;

/* Error Classes */
#define ERROR(name) static VALUE cError_##name;
#include "errors.h"
#undef ERROR

/*-----------------------------------------------------------------------------
 * Structs
 *----------------------------------------------------------------------------*/

typedef struct {
    MDB_env* env;
    int      refcount;
} Environment;

typedef struct Transaction Transaction;

typedef struct {
    VALUE        vtxn;
    Transaction* transaction;
    MDB_dbi      dbi;
    int          open;
} Database;

typedef struct Transaction {
    VALUE        venv;
    Environment* environment;
    VALUE        vparent;
    Transaction* parent, *child;
    int          refcount;
    MDB_txn*     txn;
} Transaction;

typedef struct {
    VALUE        vtxn;
    Transaction* transaction;
    MDB_cursor*  cur;
} Cursor;

typedef struct {
    Database* database;
    MDB_txn*  txn;
    int       argc;
    VALUE*    argv;
} WithTransactionArgs;

/*-----------------------------------------------------------------------------
 * Prototypes
 *----------------------------------------------------------------------------*/

static void database_check(Database*);
static void cursor_check(Cursor*);
static void transaction_check(Transaction*, int);
static int transaction_active(Transaction*);
static void transaction_deref(Transaction*);
static void transaction_mark(Transaction*);
static void transaction_do_abort(Transaction*);
static void transaction_do_commit(Transaction*);

/*-----------------------------------------------------------------------------
 * Helpers
 *----------------------------------------------------------------------------*/

#define F_STAT(name)                            \
    static VALUE stat_##name(VALUE self) {      \
        MDB_stat* stat;                         \
        Data_Get_Struct(self, MDB_stat, stat);  \
        return INT2NUM(stat->ms_##name);        \
    }
F_STAT(psize)
F_STAT(depth)
F_STAT(branch_pages)
F_STAT(leaf_pages)
F_STAT(overflow_pages)
F_STAT(entries)
#undef F_STAT

#define F_INFO(name)                                \
    static VALUE info_##name(VALUE self) {          \
        MDB_envinfo* info;                          \
        Data_Get_Struct(self, MDB_envinfo, info);   \
        return INT2NUM((size_t)info->me_##name);    \
    }
F_INFO(mapaddr)
F_INFO(mapsize)
F_INFO(last_pgno)
F_INFO(last_txnid)
F_INFO(maxreaders)
F_INFO(numreaders)
#undef F_INFO

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

/*-----------------------------------------------------------------------------
 * Environment functions
 *----------------------------------------------------------------------------*/

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

static VALUE environment_stat(VALUE self) {
    ENVIRONMENT(self, environment);
    MDB_stat* stat;
    VALUE vstat = Data_Make_Struct(cStat, MDB_stat, 0, -1, stat);
    check(mdb_env_stat(environment->env, stat));
    return vstat;
}

static VALUE environment_info(VALUE self) {
    MDB_envinfo* info;
    VALUE vinfo;

    ENVIRONMENT(self, environment);
    vinfo = Data_Make_Struct(cInfo, MDB_envinfo, 0, -1, info);
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

static VALUE environment_transaction(int argc, VALUE *argv, VALUE self) {
    ENVIRONMENT(self, environment);

    VALUE readonly;
    rb_scan_args(argc, argv, "01", &readonly);

    unsigned int flags = RTEST(readonly) ? MDB_RDONLY : 0;

    MDB_txn* txn;
    check(mdb_txn_begin(environment->env, 0, flags, &txn));

    Transaction* transaction;
    VALUE vtxn = Data_Make_Struct(cTransaction, Transaction, transaction_mark, transaction_deref, transaction);
    transaction->txn = txn;
    transaction->refcount = 1;
    transaction->vparent = Qnil;
    transaction->parent = 0;
    transaction->venv = self;
    transaction->environment = environment;
    ++environment->refcount;

    if (rb_block_given_p()) {
        int exception;
        VALUE result = rb_protect(rb_yield, vtxn, &exception);
        if (exception) {
            transaction_do_abort(transaction);
            rb_jump_tag(exception);
        }
        transaction_do_commit(transaction);
        return result;
    }
    return vtxn;
}

/*-----------------------------------------------------------------------------
 * Transaction functions
 *----------------------------------------------------------------------------*/

static int transaction_active(Transaction* transaction) {
    Transaction* parent;
    for (parent = transaction; parent; parent = parent->parent) {
         if (!parent->txn)
             return 0;
    }
    return 1;
}

static void transaction_check(Transaction* transaction, int no_children) {
    if (!transaction_active(transaction))
        rb_raise(cError, "Transaction is terminated");
    if (no_children && transaction->child)
        rb_raise(cError, "Transaction has an active child");
}

static void transaction_mark(Transaction* transaction) {
    if (!NIL_P(transaction->vparent))
            rb_gc_mark(transaction->vparent);
    rb_gc_mark(transaction->venv);
}

static void transaction_deref(Transaction *transaction) {
    if (--transaction->refcount == 0) {
        if (transaction_active(transaction))
            transaction_do_abort(transaction);
        if (transaction->parent)
            transaction_deref(transaction->parent);
        environment_deref(transaction->environment);
        free(transaction);
    }
}

static VALUE transaction_environment(VALUE self) {
    TRANSACTION(self, transaction, 0);
    return transaction->venv;
}

static VALUE transaction_parent(VALUE self) {
    TRANSACTION(self, transaction, 0);
    return transaction->vparent;
}

static void transaction_do_abort(Transaction* transaction) {
    mdb_txn_abort(transaction->txn);
    if (transaction->parent)
        transaction->parent->child = 0;
    transaction->txn = 0;
}

static void transaction_do_commit(Transaction* transaction) {
    mdb_txn_commit(transaction->txn);
    if (transaction->parent)
        transaction->parent->child = 0;
    transaction->txn = 0;
}

static VALUE transaction_abort(VALUE self) {
    TRANSACTION(self, transaction, 0);
    transaction_do_abort(transaction);
    return Qnil;
}

static VALUE transaction_commit(VALUE self) {
    TRANSACTION(self, transaction, 0);
    transaction_do_commit(transaction);
    return Qnil;
}

static VALUE transaction_renew(VALUE self) {
    TRANSACTION(self, transaction, 1);
    mdb_txn_renew(transaction->txn);
    return Qnil;
}

static VALUE transaction_reset(VALUE self) {
    TRANSACTION(self, transaction, 1);
    mdb_txn_reset(transaction->txn);
    return Qnil;
}

static VALUE transaction_transaction(int argc, VALUE* argv, VALUE self) {
    TRANSACTION(self, transaction, 1);

    VALUE readonly;
    rb_scan_args(argc, argv, "01", &readonly);

    unsigned int flags = RTEST(readonly) ? MDB_RDONLY : 0;

    MDB_txn* txn;
    check(mdb_txn_begin(mdb_txn_env(transaction->txn), transaction->txn, flags, &txn));

    Transaction* child;
    VALUE vtxn = Data_Make_Struct(cTransaction, Transaction, transaction_mark, transaction_deref, child);
    child->txn = txn;
    child->vparent = self;
    child->parent = transaction;
    child->refcount = 1;
    child->venv = transaction->venv;
    child->environment = transaction->environment;

    transaction->child = child;
    ++transaction->refcount;
    ++child->environment->refcount;

    if (rb_block_given_p()) {
        int exception;
        VALUE result = rb_protect(rb_yield, vtxn, &exception);
        if (exception) {
            transaction_do_abort(child);
            rb_jump_tag(exception);
        }
        transaction_do_commit(child);
        return result;
    }
    return vtxn;
}

/*-----------------------------------------------------------------------------
 * Database functions
 *----------------------------------------------------------------------------*/

static VALUE database_transaction(VALUE self) {
    DATABASE(self, database);
    return database->vtxn;
}

static void database_check(Database* database) {
    transaction_check(database->transaction, 1);
    if (!database->open)
        rb_raise(cError, "Database is closed");
}

static void database_free(Database* database) {
    if (database->open)
        mdb_dbi_close(database->transaction->environment->env, database->dbi);
    transaction_deref(database->transaction);
    free(database);
}

static void database_mark(Database* database) {
    rb_gc_mark(database->vtxn);
}

static VALUE database_open(int argc, VALUE *argv, VALUE self) {
    TRANSACTION(self, transaction, 0);

    VALUE vname, vflags;
    rb_scan_args(argc, argv, "11", &vname, &vflags);

    MDB_dbi dbi;
    check(mdb_dbi_open(transaction->txn, NIL_P(vname) ? 0 : StringValueCStr(vname), NIL_P(vflags) ? 0 : NUM2INT(vflags), &dbi));

    Database* database;
    VALUE vdb = Data_Make_Struct(cDatabase, Database, database_mark, database_free, database);
    database->dbi = dbi;
    database->vtxn = self;
    database->transaction = transaction;
    database->open = 1;
    ++transaction->refcount;

    return vdb;
}

static VALUE database_close(VALUE self) {
    DATABASE(self, database);
    mdb_dbi_close(database->transaction->environment->env, database->dbi);
    database->open = 0;
    return Qnil;
}

WITH_TRANSACTION(database_stat, 0, 0) {
    MDB_stat* stat;
    VALUE vstat = Data_Make_Struct(cStat, MDB_stat, 0, -1, stat);
    check(mdb_stat(a->txn, a->database->dbi, stat));
    return vstat;
}

WITH_TRANSACTION(database_drop, 0, 0) {
    check(mdb_drop(a->txn, a->database->dbi, 1));
    a->database->open = 0;
    return Qnil;
}

WITH_TRANSACTION(database_clear, 0, 0) {
    check(mdb_drop(a->txn, a->database->dbi, 0));
    return Qnil;
}

WITH_TRANSACTION(database_get, 1, 0) {
    VALUE vkey = StringValue(a->argv[0]);
    MDB_val key, value;
    key.mv_size = RSTRING_LEN(vkey);
    key.mv_data = RSTRING_PTR(vkey);

    int ret = mdb_get(a->txn, a->database->dbi, &key, &value);
    if (ret == MDB_NOTFOUND)
        return Qnil;
    check(ret);
    return rb_str_new(value.mv_data, value.mv_size);
}

WITH_TRANSACTION(database_put, 2, 1) {
    VALUE vkey = StringValue(a->argv[0]);
    VALUE vval = StringValue(a->argv[1]);

    MDB_val key, value;
    key.mv_size = RSTRING_LEN(vkey);
    key.mv_data = RSTRING_PTR(vkey);
    value.mv_size = RSTRING_LEN(vval);
    value.mv_data = RSTRING_PTR(vval);

    check(mdb_put(a->txn, a->database->dbi, &key, &value, a->argc == 3 ? NUM2INT(a->argv[2]) : 0));
    return Qnil;
}

WITH_TRANSACTION(database_delete, 1, 1) {
    VALUE vkey = StringValue(vkey);

    MDB_val key;
    key.mv_size = RSTRING_LEN(vkey);
    key.mv_data = RSTRING_PTR(vkey);

    if (a->argc == 1) {
        check(mdb_del(a->txn, a->database->dbi, &key, 0));
    } else {
        VALUE vval = StringValue(a->argv[1]);
        MDB_val value;
        value.mv_size = RSTRING_LEN(vval);
        value.mv_data = RSTRING_PTR(vval);
        check(mdb_del(a->txn, a->database->dbi, &key, &value));
    }

    return Qnil;
}

/*-----------------------------------------------------------------------------
 * Cursor functions
 *----------------------------------------------------------------------------*/

static void cursor_free(Cursor* cursor) {
    if (cursor->cur)
        mdb_cursor_close(cursor->cur);
    transaction_deref(cursor->transaction);
    free(cursor);
}

static void cursor_check(Cursor* cursor) {
    transaction_check(cursor->transaction, 1);
    if (!cursor->cur)
        rb_raise(cError, "Cursor is closed");
}

static void cursor_mark(Cursor* cursor) {
    rb_gc_mark(cursor->vtxn);
}

static VALUE cursor_close(VALUE self) {
    CURSOR(self, cursor);
    mdb_cursor_close(cursor->cur);
    cursor->cur = 0;
    return Qnil;
}

static VALUE database_cursor(int argc, VALUE* argv, VALUE self) {
    DATABASE(self, database);

    VALUE vtxn;
    rb_scan_args(argc, argv, "01", &vtxn);

    if (NIL_P(vtxn)) {
        if (!rb_block_given_p())
            rb_raise(rb_eRuntimeError, "You must either specify a transaction or give a block");
        vtxn = transaction_transaction(database->vtxn, 0, 0);
    }

    TRANSACTION(vtxn, transaction, 1);

    MDB_cursor* cur;
    check(mdb_cursor_open(transaction->txn, database->dbi, &cur));

    Cursor* cursor;
    VALUE vcur = Data_Make_Struct(cCursor, Cursor, cursor_mark, cursor_free, cursor);
    cursor->cur = cur;
    cursor->vtxn = vtxn;
    cursor->transaction = transaction;
    ++transaction->refcount;

    if (rb_block_given_p()) {
        int exception;
        VALUE result = rb_protect(rb_yield, vcur, &exception);
        if (exception) {
            cursor_close(vcur);
            mdb_txn_abort(transaction->txn);
            transaction->txn = 0;
            rb_jump_tag(exception);
        }
        cursor_close(vcur);
        mdb_txn_commit(transaction->txn);
        transaction->txn = 0;
        return result;
    }

    return vcur;
}

static VALUE cursor_transaction(VALUE self) {
    CURSOR(self, cursor);
    return cursor->vtxn;
}

static VALUE cursor_first(VALUE self) {
    CURSOR(self, cursor);
    MDB_val key, value;

    check(mdb_cursor_get(cursor->cur, &key, &value, MDB_FIRST));
    return rb_assoc_new(rb_str_new(key.mv_data, key.mv_size), rb_str_new(value.mv_data, value.mv_size));
}

static VALUE cursor_next(VALUE self) {
    CURSOR(self, cursor);
    MDB_val key, value;

    check(mdb_cursor_get(cursor->cur, &key, &value, MDB_NEXT));
    return rb_assoc_new(rb_str_new(key.mv_data, key.mv_size), rb_str_new(value.mv_data, value.mv_size));
}

static VALUE cursor_set(VALUE self, VALUE inkey) {
    CURSOR(self, cursor);
    MDB_val key, value;

    key.mv_size = RSTRING_LEN(inkey);
    key.mv_data = StringValuePtr(inkey);

    check(mdb_cursor_get(cursor->cur, &key, &value, MDB_SET));
    return rb_assoc_new(rb_str_new(key.mv_data, key.mv_size), rb_str_new(value.mv_data, value.mv_size));
}

static VALUE cursor_set_range(VALUE self, VALUE inkey) {
    CURSOR(self, cursor);
    MDB_val key, value;

    key.mv_size = RSTRING_LEN(inkey);
    key.mv_data = StringValuePtr(inkey);

    check(mdb_cursor_get(cursor->cur, &key, &value, MDB_SET_RANGE));
    return rb_assoc_new(rb_str_new(key.mv_data, key.mv_size), rb_str_new(value.mv_data, value.mv_size));
}

static VALUE cursor_get(VALUE self) {
    CURSOR(self, cursor);
    // TODO
    return Qnil;
}

static VALUE cursor_put(VALUE self) {
    CURSOR(self, cursor);
    // TODO
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

    cError = rb_define_class_under(mLMDB, "Error", rb_eRuntimeError);
#define ERROR(name) cError_##name = rb_define_class_under(cError, #name, cError);
#include "errors.h"
#undef ERROR

    cStat = rb_define_class_under(mLMDB, "Stat", rb_cObject);
    rb_define_method(cStat, "psize", stat_psize, 0);
    rb_define_method(cStat, "depth", stat_depth, 0);
    rb_define_method(cStat, "branch_pages", stat_branch_pages, 0);
    rb_define_method(cStat, "leaf_pages", stat_leaf_pages, 0);
    rb_define_method(cStat, "overflow_pages", stat_overflow_pages, 0);
    rb_define_method(cStat, "entries", stat_entries, 0);

    cInfo = rb_define_class_under(mLMDB, "Info", rb_cObject);
    rb_define_method(cInfo, "mapaddr", info_mapaddr, 0);
    rb_define_method(cInfo, "mapsize", info_mapsize, 0);
    rb_define_method(cInfo, "last_pgno", info_last_pgno, 0);
    rb_define_method(cInfo, "last_txnid", info_last_txnid, 0);
    rb_define_method(cInfo, "maxreaders", info_maxreaders, 0);
    rb_define_method(cInfo, "numreaders", info_numreaders, 0);

    cEnvironment = rb_define_class_under(mLMDB, "Environment", rb_cObject);
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

    cDatabase = rb_define_class_under(mLMDB, "Database", rb_cObject);
    rb_define_method(cDatabase, "close", database_close, 0);
    rb_define_method(cDatabase, "stat", database_stat, -1);
    rb_define_method(cDatabase, "drop", database_drop, -1);
    rb_define_method(cDatabase, "clear", database_clear, -1);
    rb_define_method(cDatabase, "get", database_get, -1);
    rb_define_method(cDatabase, "put", database_put, -1);
    rb_define_method(cDatabase, "delete", database_delete, -1);
    rb_define_method(cDatabase, "cursor", database_cursor, -1);
    rb_define_method(cDatabase, "transaction", database_transaction, 0);

    cTransaction = rb_define_class_under(mLMDB, "Transaction", rb_cObject);
    rb_define_method(cTransaction, "open", database_open, -1);
    rb_define_method(cTransaction, "abort", transaction_abort, 0);
    rb_define_method(cTransaction, "commit", transaction_commit, 0);
    rb_define_method(cTransaction, "reset", transaction_reset, 0);
    rb_define_method(cTransaction, "renew", transaction_renew, 0);
    rb_define_method(cTransaction, "transaction", transaction_transaction, -1);
    rb_define_method(cTransaction, "environment", transaction_environment, 0);
    rb_define_method(cTransaction, "parent", transaction_parent, 0);

    cCursor = rb_define_class_under(mLMDB, "Cursor", rb_cObject);
    rb_define_method(cCursor, "close", cursor_close, 0);
    rb_define_method(cCursor, "get", cursor_get, 0);
    rb_define_method(cCursor, "first", cursor_first, 0);
    rb_define_method(cCursor, "next", cursor_next, 0);
    rb_define_method(cCursor, "set", cursor_set, 1);
    rb_define_method(cCursor, "set_range", cursor_set_range, 1);
    rb_define_method(cCursor, "put", cursor_put, 0);
    rb_define_method(cCursor, "count", cursor_count, 0);
    rb_define_method(cCursor, "delete", cursor_delete, 0);
    rb_define_method(cCursor, "transaction", cursor_transaction, 0);
}
