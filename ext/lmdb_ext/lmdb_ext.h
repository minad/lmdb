#ifndef _LMDB_EXT_H
#define _LMDB_EXT_H

#include "ruby.h"
#include "lmdb.h"

// Ruby 1.8 compatibility
#ifndef SIZET2NUM
#  if SIZEOF_SIZE_T > SIZEOF_LONG && defined(HAVE_LONG_LONG)
#   define SIZET2NUM(v) ULL2NUM(v)
#  elif SIZEOF_SIZE_T == SIZEOF_LONG
#   define SIZET2NUM(v) ULONG2NUM(v)
#  else
#   define SIZET2NUM(v) UINT2NUM(v)
#  endif
#endif

// Ruby 1.8 compatibility
#ifndef NUM2SSIZET
#  if defined(HAVE_LONG_LONG) && SIZEOF_SIZE_T > SIZEOF_LONG
#   define NUM2SSIZET(x) ((ssize_t)NUM2LL(x))
#else
#   define NUM2SSIZET(x) NUM2LONG(x)
#  endif
#endif

// Ruby 2.0 compatibility
#ifndef RARRAY_AREF
#  define RARRAY_AREF(ary,n) (RARRAY_PTR(ary)[n])
#endif

#define ENVIRONMENT(var, var_env)                       \
        Environment* var_env;                           \
        Data_Get_Struct(var, Environment, var_env);     \
        environment_check(var_env)

#define DATABASE(var, var_db)                   \
        Database* var_db;                       \
        Data_Get_Struct(var, Database, var_db);

#define TRANSACTION(var, var_txn)                       \
        Transaction* var_txn;                           \
        Data_Get_Struct(var, Transaction, var_txn)

#define CURSOR(var, var_cur)                    \
        Cursor* var_cur;                        \
        Data_Get_Struct(var, Cursor, var_cur);  \
        cursor_check(var_cur)

typedef struct {
        VALUE    env;
        VALUE    parent;
        VALUE    thread;
        VALUE    cursors;
        MDB_txn* txn;
} Transaction;

typedef struct {
        MDB_env* env;
        VALUE    thread_txn_hash;
        VALUE    txn_thread_hash;
} Environment;

typedef struct {
        VALUE   env;
        MDB_dbi dbi;
} Database;

typedef struct {
        VALUE       db;
        MDB_cursor* cur;
} Cursor;

typedef struct {
        VALUE self;
        const char* name;
        int argc;
        const VALUE* argv;
} HelperArgs;

typedef struct {
        mode_t mode;
        int    flags;
        int    maxreaders;
        int    maxdbs;
        size_t mapsize;
} EnvironmentOptions;

typedef struct {
        MDB_env *env;
        MDB_txn *parent;
        unsigned int flags;
        MDB_txn **htxn;
        int result;
        int stop;
} TxnArgs;

static VALUE cEnvironment, cDatabase, cTransaction, cCursor, cError;

#define ERROR(name) static VALUE cError_##name;
#include "errors.h"
#undef ERROR

// BEGIN PROTOTYPES
void Init_lmdb_ext();
static MDB_txn* active_txn(VALUE self);
static VALUE call_with_transaction(VALUE venv, VALUE self, const char* name, int argc, const VALUE* argv, int flags);
static VALUE call_with_transaction_helper(VALUE arg);
static void check(int code);
static void cursor_check(Cursor* cursor);
static VALUE cursor_close(VALUE self);
static VALUE cursor_count(VALUE self);
static VALUE cursor_delete(int argc, VALUE *argv, VALUE self);
static VALUE cursor_first(VALUE self);
static void cursor_free(Cursor* cursor);
static VALUE cursor_get(VALUE self);
static VALUE cursor_last(VALUE self);
static void cursor_mark(Cursor* cursor);
static VALUE cursor_next(VALUE self);
static VALUE cursor_prev(VALUE self);
static VALUE cursor_put(int argc, VALUE* argv, VALUE self);
static VALUE cursor_set(int argc, VALUE* argv, VALUE self);
static VALUE cursor_set_range(VALUE self, VALUE vkey);
static VALUE database_clear(VALUE self);
static VALUE database_cursor(VALUE self);
static VALUE database_delete(int argc, VALUE *argv, VALUE self);
static VALUE database_drop(VALUE self);
static VALUE database_get(VALUE self, VALUE vkey);
static void database_mark(Database* database);
static VALUE database_put(int argc, VALUE *argv, VALUE self);
static VALUE database_stat(VALUE self);
static VALUE database_get_flags(VALUE self);
static VALUE environment_active_txn(VALUE self);
static VALUE environment_change_flags(int argc, VALUE* argv, VALUE self, int set);
static void environment_check(Environment* environment);
static VALUE environment_clear_flags(int argc, VALUE* argv, VALUE self);
static VALUE environment_close(VALUE self);
static VALUE environment_copy(VALUE self, VALUE path);
static VALUE environment_database(int argc, VALUE *argv, VALUE self);
static VALUE environment_flags(VALUE self);
static void environment_free(Environment *environment);
static VALUE environment_info(VALUE self);
static void environment_mark(Environment* environment);
static VALUE environment_new(int argc, VALUE *argv, VALUE klass);
static int environment_options(VALUE key, VALUE value, EnvironmentOptions* options);
static VALUE environment_path(VALUE self);
static void environment_set_active_txn(VALUE self, VALUE thread, VALUE txn);
static VALUE environment_set_flags(int argc, VALUE* argv, VALUE self);
static VALUE environment_stat(VALUE self);
static VALUE environment_sync(int argc, VALUE *argv, VALUE self);
static VALUE environment_transaction(int argc, VALUE *argv, VALUE self);
static MDB_txn* need_txn(VALUE self);
static VALUE stat2hash(const MDB_stat* stat);
static VALUE transaction_abort(VALUE self);
static VALUE transaction_commit(VALUE self);
static void transaction_finish(VALUE self, int commit);
static void transaction_free(Transaction* transaction);
static void transaction_mark(Transaction* transaction);
static VALUE with_transaction(VALUE venv, VALUE(*fn)(VALUE), VALUE arg, int flags);
// END PROTOTYPES

#endif
