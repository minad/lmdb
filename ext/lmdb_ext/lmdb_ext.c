#include "lmdb_ext.h"
#include "extconf.h"

#ifdef HAVE_RB_THREAD_CALL_WITHOUT_GVL2

// ruby 2
#include "ruby/thread.h"
#define CALL_WITHOUT_GVL(func, data1, ubf, data2) \
  rb_thread_call_without_gvl2(func, data1, ubf, data2)

#else

// ruby 193
// Expose the API from internal.h:
VALUE rb_thread_call_without_gvl(
    rb_blocking_function_t *func, void *data1,
    rb_unblock_function_t *ubf, void *data2);
#define CALL_WITHOUT_GVL(func, data1, ubf, data2) \
  rb_thread_call_without_gvl((rb_blocking_function_t *)func, data1, ubf, data2)

#endif

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

static void transaction_free(Transaction* transaction) {
        if (transaction->txn) {
                rb_warn("Memory leak - Garbage collecting active transaction");
                // mdb_txn_abort(transaction->txn);
        }
        free(transaction);
}

static void transaction_mark(Transaction* transaction) {
        rb_gc_mark(transaction->parent);
        rb_gc_mark(transaction->env);
        rb_gc_mark(transaction->cursors);
}

/**
 * Commit a transaction in process.  Any subtransactions of this
 * transaction will be committed as well.
 *
 * One does not normally need to call commit explicitly; a
 * commit is performed automatically when the block supplied to
 * {Environment#transaction} exits normally.
 *
 * @note After committing a transaction, no further database operations
 *    should be done in the block.  Any cursors created in the context
 *    of the transaction will no longer be valid.
 *
 * @example Single transaction
 *    env.transaction do |txn|
 *      # ... modify the databases ...
 *      txn.commit
 *    end
 *
 * @example Child transactions
 *    env.transaction do |txn1|
 *      env.transaction.do |txn2|
 *         txn1.commit      # txn1 and txn2 are both committed
 *      end
 *    end
 */
static VALUE transaction_commit(VALUE self) {
        transaction_finish(self, 1);
        return Qnil;
}

/**
 * Abort a transaction in process. Any subtransactions of this
 * transaction will be aborted as well.
 *
 * @note After aborting a transaction, no further database operations
 *    should be done in the block.  Any cursors created in the context
 *    of the transaction will no longer be valid.
 *
 * @example Single transaction
 *    env.transaction do |txn|
 *      # ... modify the databases ...
 *      txn.abort
 *      # modifications are rolled back
 *    end
 *
 * @example Child transactions
 *    env.transaction do |txn1|
 *      env.transaction.do |txn2|
 *         txn1.abort      # txn1 and txn2 are both aborted
 *      end
 *    end
 */
static VALUE transaction_abort(VALUE self) {
        transaction_finish(self, 0);
        return Qnil;
}

/**
 * @overload transaction_env
 *   @return [Environment] the environment in which this transaction is running.
 *   @example
 *      env.transaction do |t|
 *        env == t.env
 *        # should be true
 *      end
 */
static VALUE transaction_env(VALUE self) {
        TRANSACTION(self, transaction);
        return transaction->env;
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

        long i;
        for (i=0; i<RARRAY_LEN(transaction->cursors); i++) {
                VALUE cursor = RARRAY_AREF(transaction->cursors, i);
                cursor_close(cursor);
        }
        rb_ary_clear(transaction->cursors);

        // Mark child transactions as closed
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
static VALUE call_with_transaction_helper(VALUE arg) {
        #error "Not implemented"
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

static void *call_txn_begin(void *arg) {
        TxnArgs *txn_args = arg;
        txn_args->result = mdb_txn_begin(txn_args->env,
          txn_args->parent, txn_args->flags, txn_args->htxn);
        if (txn_args->result == MDB_MAP_RESIZED) {
            check(mdb_env_set_mapsize(txn_args->env, 0));
            txn_args->result = mdb_txn_begin(txn_args->env,
              txn_args->parent, txn_args->flags, txn_args->htxn);
        }
        return (void *)NULL;
}

static void stop_txn_begin(void *arg)
{
        TxnArgs *txn_args = arg;
        // There's no way to stop waiting for mutex:
        //   http://www.cognitus.net/faq/pthread/pthreadSemiFAQ_6.html
        // However, we can (and must) release the mutex as soon as we get it:
        txn_args->stop = 1;
}

static VALUE with_transaction(VALUE venv, VALUE(*fn)(VALUE), VALUE arg, int flags) {
        ENVIRONMENT(venv, environment);
        if(environment->flags & MDB_RDONLY)
          flags |= MDB_RDONLY;

        MDB_txn* txn;
        TxnArgs txn_args;

    retry:
        txn = NULL;

        txn_args.env = environment->env;
        txn_args.parent = active_txn(venv);
        txn_args.flags = flags;
        txn_args.htxn = &txn;
        txn_args.result = 0;
        txn_args.stop = 0;

        if (flags & MDB_RDONLY) {
                call_txn_begin(&txn_args);
        }
        else {
                CALL_WITHOUT_GVL(
                    call_txn_begin, &txn_args,
                    stop_txn_begin, &txn_args);

                if (txn_args.stop || !txn) {
                        // !txn is when rb_thread_call_without_gvl2
                        // returns before calling txn_begin
                        if (txn) {
                                mdb_txn_abort(txn);
                        }
                        rb_thread_check_ints();
                        goto retry; // in what cases do we get here?
                }
        }

        check(txn_args.result);

        Transaction* transaction;
        VALUE vtxn = Data_Make_Struct(cTransaction, Transaction, transaction_mark, transaction_free, transaction);
        transaction->parent = environment_active_txn(venv);
        transaction->env = venv;
        transaction->txn = txn;
        transaction->thread = rb_thread_current();
        transaction->cursors = rb_ary_new();
        environment_set_active_txn(venv, transaction->thread, vtxn);

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

static void environment_free(Environment *environment) {
        if (environment->env) {
                // rb_warn("Memory leak - Garbage collecting open environment");
                if (!RHASH_EMPTY_P(environment->txn_thread_hash)) {
                    // If a transaction (or cursor) is open, its block is on the
                    // stack, so it will not be collected, so environment_free
                    // should not be called.
                    rb_warn("Bug: closing environment with open transactions.");
                }
                mdb_env_close(environment->env);
        }
        free(environment);
}


static void environment_mark(Environment* environment) {
        rb_gc_mark(environment->thread_txn_hash);
        rb_gc_mark(environment->txn_thread_hash);
}

/**
 * @overload close
 *   Close an environment, completing all IOs and cleaning up database
 *   state if needed.
 *   @example
 *      env = LMDB.new('abc')
 *      # ...various operations on the environment...
 *      env.close
 */
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

/**
 * @overload stat
 *   Return useful statistics about an environment.
 *   @return [Hash] the statistics
 *   * +:psize+ Size of a database page
 *   * +:depth+ Depth (height) of the B-tree
 *   * +:branch_pages+ Number of internal (non-leaf) pages
 *   * +:leaf_pages+ Number of leaf pages
 *   * +:overflow_pages+ Number of overflow pages
 *   * +:entries+ Number of data items
 */
static VALUE environment_stat(VALUE self) {
        ENVIRONMENT(self, environment);
        MDB_stat stat;
        check(mdb_env_stat(environment->env, &stat));
        return stat2hash(&stat);
}

/**
 * @overload info
 *   Return useful information about an environment.
 *   @return [Hash]
 *   * +:mapaddr+ The memory address at which the database is mapped, if fixed
 *   * +:mapsize+ The size of the data memory map
 *   * +:last_pgno+ ID of the last used page
 *   * +:last_txnid+ ID of the last committed transaction
 *   * +:maxreaders+ Max reader slots in the environment
 *   * +:numreaders+ Max readers slots in the environment
 */
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

/**
 * @overload copy(path)
 *   Create a copy (snapshot) of an environment.  The copy can be used
 *   as a backup.  The copy internally uses a read-only transaction to
 *   ensure that the copied data is serialized with respect to database
 *   updates.
 *   @param [String] path The directory in which the copy will
 *       reside. This directory must already exist and be writable but
 *       must otherwise be empty.
 *   @return nil
 *   @raise [Error] when there is an error creating the copy.
 */
static VALUE environment_copy(VALUE self, VALUE path) {
        ENVIRONMENT(self, environment);
        VALUE expanded_path = rb_file_expand_path(path, Qnil);
        check(mdb_env_copy(environment->env, StringValueCStr(expanded_path)));
        return Qnil;
}

/**
 * @overload sync(force)
 *   Flush the data buffers to disk.
 *
 *   Data is always written to disk when {Transaction#commit} is called, but
 *   the operating system may keep it buffered. MDB always flushes the
 *   OS buffers upon commit as well, unless the environment was opened
 *   with +:nosync+ or in part +:nometasync+.
 *   @param [Boolean] force If true, force a synchronous
 *     flush. Otherwise if the environment has the +:nosync+ flag set
 *     the flushes will be omitted, and with +:mapasync+ they will be
 *     asynchronous.
 */
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

/**
 * @overload new(path, opts)
 *   Open an LMDB database environment.
 *   The database environment is the root object for all operations on
 *   a collection of databases.  It has to be opened first, before
 *   individual databases can be opened or created in the environment.
 *   The database should be closed when it is no longer needed.
 *
 *   The options hash on this method includes all the flags listed in
 *   {Environment#flags} as well as the options documented here.
 *   @return [Environment]
 *   @param [String] path the path to the files containing the database
 *   @param [Hash] opts options for the database environment
 *   @option opts [Number] :mode The Posix permissions to set on created files.
 *   @option opts [Number] :maxreaders The maximum number of concurrent threads
 *       that can be executing transactions at once.  Default is 126.
 *   @option opts [Number] :maxdbs The maximum number of named databases in the
 *       environment.  Not needed if only one database is being used.
 *   @option opts [Number] :mapsize The size of the memory map to be allocated
 *       for this environment, in bytes.  The memory map size is the
 *       maximum total size of the database.  The size should be a
 *       multiple of the OS page size.  The default size is about
 *       10MiB.
 *   @yield [env] The block to be executed with the environment. The environment is closed afterwards.
 *   @yieldparam env [Environment] The environment
 *   @see #close
 *   @see Environment#flags
 *   @example Open environment and pass options
 *      env = LMDB.new "dbdir", :maxdbs => 30, :mapasync => true, :writemap => true
 *   @example Pass environment to block
 *      LMDB.new "dbdir" do |env|
 *        # ...
 *      end
 */
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
        VALUE venv = Data_Make_Struct(cEnvironment, Environment, environment_mark, environment_free, environment);
        environment->env = env;
        environment->thread_txn_hash = rb_hash_new();
        environment->txn_thread_hash = rb_hash_new();
        environment->flags = options.flags;

        if (options.maxreaders > 0)
                check(mdb_env_set_maxreaders(env, options.maxreaders));
        if (options.mapsize > 0)
                check(mdb_env_set_mapsize(env, options.mapsize));

        check(mdb_env_set_maxdbs(env, options.maxdbs <= 0 ? 1 : options.maxdbs));
        VALUE expanded_path = rb_file_expand_path(path, Qnil);
        check(mdb_env_open(env, StringValueCStr(expanded_path), options.flags, options.mode));

        if (rb_block_given_p())
                return rb_ensure(rb_yield, venv, environment_close, venv);

        return venv;
}

/**
 * @overload flags
 *   Return the flags that are set in this environment.
 *   @return [Array] Array of flag symbols
 *   The environment flags are:
 *   * +:fixedmap+ Use a fixed address for the mmap region.
 *   * +:nosubdir+ By default, MDB creates its environment in a directory whose pathname is given in +path+, and creates its data and lock files under that directory. With this option, path is used as-is for the database main data file. The database lock file is the path with "-lock" appended.
 *   * +:nosync+ Don't flush system buffers to disk when committing a transaction. This optimization means a system crash can corrupt the database or lose the last transactions if buffers are not yet flushed to disk. The risk is governed by how often the system flushes dirty buffers to disk and how often {Environment#sync} is called. However, if the filesystem preserves write order and the +:writemap+ flag is not used, transactions exhibit ACI (atomicity, consistency, isolation) properties and only lose D (durability). That is, database integrity is maintained, but a system crash may undo the final transactions. Note that +:nosync + :writemap+ leaves the system with no hint for when to write transactions to disk, unless {Environment#sync} is called. +:mapasync + :writemap+ may be preferable.
 *   * +:rdonly+ Open the environment in read-only mode. No write operations will be allowed. MDB will still modify the lock file - except on read-only filesystems, where MDB does not use locks.
 *   * +:nometasync+ Flush system buffers to disk only once per transaction, omit the metadata flush. Defer that until the system flushes files to disk, or next  non-MDB_RDONLY commit or {Environment#sync}. This optimization maintains database integrity, but a system crash may undo the last committed transaction. That is, it preserves the ACI (atomicity, consistency, isolation) but not D (durability) database property.
 *   * +:writemap+ Use a writeable memory map unless +:rdonly+ is set. This is faster and uses fewer mallocs, but loses protection from application bugs like wild pointer writes and other bad updates into the database. Incompatible with nested transactions.
 *   * +:mapasync+ When using +:writemap+, use asynchronous flushes to disk. As with +:nosync+, a system crash can then corrupt the database or lose the last transactions. Calling {Environment#sync} ensures on-disk database integrity until next commit.
 *   * +:notls+ Don't use thread-local storage.
 *   @example
 *       env = LMDB.new "abc", :writemap => true, :nometasync => true
 *       env.flags           #=> [:writemap, :nometasync]
 */
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

/**
 * @overload path
 *   Return the path to the database environment files
 *   @return [String] the path that was used to open the environment.
 */
static VALUE environment_path(VALUE self) {
        const char* path;
        ENVIRONMENT(self, environment);
        check(mdb_env_get_path(environment->env, &path));
        return rb_str_new2(path);
}

static VALUE environment_set_mapsize(VALUE self, VALUE size) {
        ENVIRONMENT(self, environment);
        check(mdb_env_set_mapsize(environment->env, NUM2LONG(size)));
        return Qnil;
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

/**
 * @overload set_flags(flags)
 *   Set one or more flags in the environment. The available flags are defined in {Environment#flags}.
 *   @see Environment#flags
 *   @param [Array] flags Array of flag names (symbols) to set
 *   @return nil
 *   @raise [Error] if an invalid flag name is specified
 *   @example
 *    env.set_flags :nosync, :writemap
 */
static VALUE environment_set_flags(int argc, VALUE* argv, VALUE self) {
        environment_change_flags(argc, argv, self, 1);
        return Qnil;
}

/**
 * @overload clear_flags(flags)
 *   Clear one or more flags in the environment. The available flags are defined in {Environment#flags}.
 *   @see Environment#flags
 *   @param [Array] flags Array of flag names (symbols) to clear
 *   @return nil
 *   @raise [Error] if an invalid flag name is specified
 *   @example
 *     env.clear_flags :nosync, :writemap
 */
static VALUE environment_clear_flags(int argc, VALUE* argv, VALUE self) {
        environment_change_flags(argc, argv, self, 0);
        return Qnil;
}

/**
 * @overload active_txn
 *   @return [Transaction] the current active transaction on this thread in the environment.
 *   @example
 *      env.transaction do |t|
 *        active = env.active_txn
 *        # active should equal t
 *      end
 */
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
                VALUE oldtxn = rb_hash_aref(environment->thread_txn_hash, thread);
                if (!NIL_P(oldtxn)) {
                        rb_hash_delete(environment->txn_thread_hash, oldtxn);
                }
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

/**
 * @overload transaction(readonly)
 *   Begin a transaction.  Takes a block to run the body of the
 *   transaction.  A transaction commits when it exits the block successfully.
 *   A transaction aborts when it raises an exception or calls
 *   {Transaction#abort}.
 *   @param [Boolean] readonly This transaction will not perform any
 *      write operations
 *   @note Transactions can be nested.
 *   @yield [txn] The block to be executed with the body of the transaction.
 *   @yieldparam txn [Transaction] An optional transaction argument
 *   @example
 *      db = env.database "mydata"
 *      env.transaction do |txn1|
 *        db['a'] = 1
 *        env.transaction do |txn2|
 *          # txn2 is nested in txn1
 *          db['a'] = 2
 *          db['a']                    #=> 2
 *          txn2.abort
 *        end
 *        db['a']                      #=> 1
 *        env.transaction do
 *          db['a'] = 3
 *        end
 *      end
 *      db['a']                        #=> 3
 */
static VALUE environment_transaction(int argc, VALUE *argv, VALUE self) {
        rb_need_block();

        VALUE readonly;
        rb_scan_args(argc, argv, "01", &readonly);
        unsigned int flags = RTEST(readonly) ? MDB_RDONLY : 0;

        return with_transaction(self, rb_yield, Qnil, flags);
}

static void database_mark(Database* database) {
        rb_gc_mark(database->env);
}

#define METHOD database_flags
#define FILE "dbi_flags.h"
#include "flag_parser.h"
#undef METHOD
#undef FILE

/**
 * @overload database(name, options)
 *   Opens a database within the environment.
 *
 *   Note that a database is opened or created within a transaction.  If
 *   the open creates a new database, the database is not available for
 *   other operations in other transactions until the transaction that
 *   is creating the database commits.  If the transaction creating the
 *   database aborts, the database is not created.
 *   @return [Database] newly-opened database
 *   @raise [Error] if there is an error opening the database
 *   @param [String] name Optional name for the database to be opened.
 *   @param [Hash] options Options for the database.
 *   @option options [Boolean] :reversekey Keys are strings to be
 *       compared in reverse order, from the end of the strings to the
 *       beginning. By default, Keys are treated as strings and
 *       compared from beginning to end.
 *   @option options [Boolean] :dupsort Duplicate keys may be used in
 *       the database. (Or, from another perspective, keys may have
 *       multiple data items, stored in sorted order.) By default keys
 *       must be unique and may have only a single data item.
 *   @option options [Boolean] :integerkey Keys are binary integers in
 *       native byte order.
 *   @option options [Boolean] :dupfixed This flag may only be used in
 *       combination with +:dupsort+. This option tells the library
 *       that the data items for this database are all the same size,
 *       which allows further optimizations in storage and retrieval.
 *   @option options [Boolean] :integerdup This option specifies that
 *       duplicate data items are also integers, and should be sorted
 *       as such.
 *   @option options [Boolean] :reversedup This option specifies that
 *       duplicate data items should be compared as strings in reverse
 *       order.
 *   @option options [Boolean] :create Create the named database if it
 *       doesn't exist. This option is not allowed in a read-only
 *       transaction or a read-only environment.
 */
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
        VALUE vdb = Data_Make_Struct(cDatabase, Database, database_mark, free, database);
        database->dbi = dbi;
        database->env = self;

        return vdb;
}

/**
 * @overload stat
 *   Return useful statistics about a database.
 *   @return [Hash] the statistics
 *   * +:psize+ Size of a database page
 *   * +:depth+ Depth (height) of the B-tree
 *   * +:branch_pages+ Number of internal (non-leaf) pages
 *   * +:leaf_pages+ Number of leaf pages
 *   * +:overflow_pages+ Number of overflow pages
 *   * +:entries+ Number of data items
 */
static VALUE database_stat(VALUE self) {
        DATABASE(self, database);
        if (!active_txn(database->env))
                return call_with_transaction(database->env, self, "stat", 0, 0, MDB_RDONLY);

        MDB_stat stat;
        check(mdb_stat(need_txn(database->env), database->dbi, &stat));
        return stat2hash(&stat);
}

/**
 * @overload drop
 *   Remove a database from the environment.
 *   @return nil
 *   @note The drop happens transactionally.
 */
static VALUE database_drop(VALUE self) {
        DATABASE(self, database);
        if (!active_txn(database->env))
                return call_with_transaction(database->env, self, "drop", 0, 0, 0);
        check(mdb_drop(need_txn(database->env), database->dbi, 1));
        return Qnil;
}

/**
 * @overload clear
 *    Empty out the database
 *    @return nil
 *    @note The clear happens transactionally.
 */
static VALUE database_clear(VALUE self) {
        DATABASE(self, database);
        if (!active_txn(database->env))
                return call_with_transaction(database->env, self, "clear", 0, 0, 0);
        check(mdb_drop(need_txn(database->env), database->dbi, 0));
        return Qnil;
}

/**
 * @overload get(key)
 *   Retrieves one value associated with this key.
 *   This function retrieves key/data pairs from the database.  If the
 *   database supports duplicate keys (+:dupsort+) then the first data
 *   item for the key will be returned. Retrieval of other items
 *   requires the use of {#cursor}.
 *   @param key The key of the record to retrieve.
 */
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

/**
 * @overload put(key, value, options)
 *   Stores items into a database.
 *   This function stores key/value pairs in the database. The default
 *   behavior is to enter the new key/value pair, replacing any
 *   previously existing key if duplicates are disallowed, or adding a
 *   duplicate data item if duplicates are allowed (+:dupsort+).
 *   @param key The key of the record to set
 *   @param value The value to insert for this key
 *   @option options [Boolean] :nodupdata Enter the new key/value
 *       pair only if it does not already appear in the database. This
 *       flag may only be specified if the database was opened with
 *       +:dupsort+. The function will raise an {Error} if the
 *       key/data pair already appears in the database.
 *   @option options [Boolean] :nooverwrite Enter the new key/value
 *       pair only if the key does not already appear in the
 *       database. The function will raise an {Error] if the key
 *       already appears in the database, even if the database
 *       supports duplicates (+:dupsort+).
 *   @option options [Boolean] :append Append the given key/data pair
 *       to the end of the database. No key comparisons are
 *       performed. This option allows fast bulk loading when keys are
 *       already known to be in the correct order. Loading unsorted
 *       keys with this flag will cause data corruption.
 *   @option options [Boolean] :appenddup As above, but for sorted dup
 *       data.
 */
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

/**
 * @overload delete(key, value=nil)
 *
 * Deletes records from the database.  This function removes
 * key/data pairs from the database. If the database does not support
 * sorted duplicate data items (+:dupsort+) the value parameter is
 * ignored. If the database supports sorted duplicates and the value
 * parameter is +nil+, all of the duplicate data items for the key will
 * be deleted. Otherwise, if the data parameter is non-nil only the
 * matching data item will be deleted.
 *
 * @param key The key of the record to delete.
 * @param value The optional value of the record to delete.
 * @raise [Error] if the specified key/value pair is not in the database.
 */
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
                vval = StringValue(vval);
                MDB_val value;
                value.mv_size = RSTRING_LEN(vval);
                value.mv_data = RSTRING_PTR(vval);
                check(mdb_del(need_txn(database->env), database->dbi, &key, &value));
        }

        return Qnil;
}

static void cursor_free(Cursor* cursor) {
        if (cursor->cur) {
                rb_warn("Memory leak - Garbage collecting open cursor");
                // mdb_cursor_close(cursor->cur);
        }

        free(cursor);
}

static void cursor_check(Cursor* cursor) {
        if (!cursor->cur)
                rb_raise(cError, "Cursor is closed");
}

static void cursor_mark(Cursor* cursor) {
        rb_gc_mark(cursor->db);
}

/**
 * @overload close
 *  Close a cursor.  The cursor must not be used again after this call.
 */
static VALUE cursor_close(VALUE self) {
        CURSOR(self, cursor);
        mdb_cursor_close(cursor->cur);
        cursor->cur = 0;
        return Qnil;
}

/**
 * @overload cursor
 *   Create a cursor to iterate through a database. Uses current
 *   transaction, if any. Otherwise, if called with a block,
 *   creates a new transaction for the scope of the block.
 *   Otherwise, fails.
 *
 *   @see Cursor
 *   @yield [cursor] A block to be executed with the cursor.
 *   @yieldparam cursor [Cursor] The cursor to be used to iterate
 *   @example
 *    db = env.database "abc"
 *    db.cursor do |c|
 *      key, value = c.next
 *      puts "#{key}: #{value}"
 *    end
 */
static VALUE database_cursor(VALUE self) {
        DATABASE(self, database);
        if (!active_txn(database->env)) {
                if (!rb_block_given_p()) {
                        rb_raise(cError, "Must call with block or active transaction.");
                }
                return call_with_transaction(database->env, self, "cursor", 0, 0, 0);
        }

        MDB_cursor* cur;
        check(mdb_cursor_open(need_txn(database->env), database->dbi, &cur));

        Cursor* cursor;
        VALUE vcur = Data_Make_Struct(cCursor, Cursor, cursor_mark, cursor_free, cursor);
        cursor->cur = cur;
        cursor->db = self;

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
        else {
                VALUE vtxn = environment_active_txn(database->env);
                if (NIL_P(vtxn)) {
                        rb_fatal("Internal error: transaction finished unexpectedly.");
                }
                else {
                        TRANSACTION(vtxn, txn);
                        rb_ary_push(txn->cursors, vcur);
                }
        }

        return vcur;
}

/**
 * @overload database_env
 *   @return [Environment] the environment to which this database belongs.
 */
static VALUE database_env(VALUE self) {
        DATABASE(self, database);
        return database->env;
}

/**
 * @overload first
 *    Position the cursor to the first record in the database, and
 *    return its value.
 *    @return [Array,nil] The [key, value] pair for the first record, or
 *        nil if no record
 */
static VALUE cursor_first(VALUE self) {
        CURSOR(self, cursor);
        MDB_val key, value;

        check(mdb_cursor_get(cursor->cur, &key, &value, MDB_FIRST));
        return rb_assoc_new(rb_str_new(key.mv_data, key.mv_size), rb_str_new(value.mv_data, value.mv_size));
}

/**
 * @overload last
 *    Position the cursor to the last record in the database, and
 *    return its value.
 *    @return [Array,nil] The [key, value] pair for the last record, or
 *        nil if no record.
 */
static VALUE cursor_last(VALUE self) {
        CURSOR(self, cursor);
        MDB_val key, value;

        check(mdb_cursor_get(cursor->cur, &key, &value, MDB_LAST));
        return rb_assoc_new(rb_str_new(key.mv_data, key.mv_size), rb_str_new(value.mv_data, value.mv_size));
}

/**
 * @overload prev
 *    Position the cursor to the previous record in the database, and
 *    return its value.
 *    @return [Array,nil] The [key, value] pair for the previous record, or
 *        nil if no previous record.
 */
static VALUE cursor_prev(VALUE self) {
        CURSOR(self, cursor);
        MDB_val key, value;

        int ret = mdb_cursor_get(cursor->cur, &key, &value, MDB_PREV);
        if (ret == MDB_NOTFOUND)
                return Qnil;
        check(ret);
        return rb_assoc_new(rb_str_new(key.mv_data, key.mv_size), rb_str_new(value.mv_data, value.mv_size));
}

/**
 * @overload next
 *    Position the cursor to the next record in the database, and
 *    return its value.
 *    @return [Array,nil] The [key, value] pair for the next record, or
 *        nil if no next record.
 */
static VALUE cursor_next(VALUE self) {
        CURSOR(self, cursor);
        MDB_val key, value;

        int ret = mdb_cursor_get(cursor->cur, &key, &value, MDB_NEXT);
        if (ret == MDB_NOTFOUND)
                return Qnil;
        check(ret);
        return rb_assoc_new(rb_str_new(key.mv_data, key.mv_size), rb_str_new(value.mv_data, value.mv_size));
}

/**
 * @overload next_range
 *    Position the cursor to the next record in the database, and
 *    return its value if the record's key is less than or equal to
 *    the specified key, or nil otherwise.
 *    @return [Array,nil] The [key, value] pair for the next record, or
 *        nil if no next record or the next record is out of the range.
 */
static VALUE cursor_next_range(VALUE self, VALUE upper_bound_key) {
        CURSOR(self, cursor);
        MDB_val key, value, ub_key;

        int ret = mdb_cursor_get(cursor->cur, &key, &value, MDB_NEXT);
        if (ret == MDB_NOTFOUND)
                return Qnil;
        check(ret);

        ub_key.mv_size = RSTRING_LEN(upper_bound_key);
        ub_key.mv_data = StringValuePtr(upper_bound_key);

        MDB_txn *txn = mdb_cursor_txn(cursor->cur);
        MDB_dbi dbi = mdb_cursor_dbi(cursor->cur);

        if (mdb_cmp(txn, dbi, &key, &ub_key) <= 0) {
            return rb_assoc_new(rb_str_new(key.mv_data, key.mv_size), rb_str_new(value.mv_data, value.mv_size));
        } else {
            return Qnil;
        }
}

/**
 * @overload set(key)
 *   Set the cursor to a specified key
 *   @param key The key to which the cursor should be positioned
 *   @return [Array] The [key, value] pair to which the cursor now points.
 */
static VALUE cursor_set(VALUE self, VALUE vkey) {
        CURSOR(self, cursor);
        MDB_val key, value;

        key.mv_size = RSTRING_LEN(vkey);
        key.mv_data = StringValuePtr(vkey);

        check(mdb_cursor_get(cursor->cur, &key, &value, MDB_SET_KEY));
        return rb_assoc_new(rb_str_new(key.mv_data, key.mv_size), rb_str_new(value.mv_data, value.mv_size));
}

/**
 * @overload set_range(key)
 *   Set the cursor at the first key greater than or equal to a specified key.
 *   @param key The key to which the cursor should be positioned
 *   @return [Array] The [key, value] pair to which the cursor now points.
 */
static VALUE cursor_set_range(VALUE self, VALUE vkey) {
        CURSOR(self, cursor);
        MDB_val key, value;

        key.mv_size = RSTRING_LEN(vkey);
        key.mv_data = StringValuePtr(vkey);

        check(mdb_cursor_get(cursor->cur, &key, &value, MDB_SET_RANGE));
        return rb_assoc_new(rb_str_new(key.mv_data, key.mv_size), rb_str_new(value.mv_data, value.mv_size));
}

/**
 * @overload get
 *    Return the value of the record to which the cursor points.
 *    @return [Array] The [key, value] pair for the current record.
 */

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

/**
 * @overload put(key, value, options)
 *   Store by cursor.  This function stores key/data pairs into the
 *   database. If the function fails for any reason, the state of
 *   the cursor will be unchanged. If the function succeeds and an
 *   item is inserted into the database, the cursor is always
 *   positioned to refer to the newly inserted item.
 *   @return nil
 *   @param key The key of the record to set
 *   @param value The value to insert for this key
 *   @option options [Boolean] :current Overwrite the data of the
 *       key/data pair to which the cursor refers with the specified
 *       data item. The +key+ parameter is ignored.
 *   @option options [Boolean] :nodupdata Enter the new key/value
 *       pair only if it does not already appear in the database. This
 *       flag may only be specified if the database was opened with
 *       +:dupsort+. The function will raise an {Error} if the
 *       key/data pair already appears in the database.
 *   @option options [Boolean] :nooverwrite Enter the new key/value
 *       pair only if the key does not already appear in the
 *       database. The function will raise an {Error] if the key
 *       already appears in the database, even if the database
 *       supports duplicates (+:dupsort+).
 *   @option options [Boolean] :append Append the given key/data pair
 *       to the end of the database. No key comparisons are
 *       performed. This option allows fast bulk loading when keys are
 *       already known to be in the correct order. Loading unsorted
 *       keys with this flag will cause data corruption.
 *   @option options [Boolean] :appenddup As above, but for sorted dup
 *       data.
 */
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

/**
 * @overload delete(options)
 *    Delete current key/data pair.
 *    This function deletes the key/data pair to which the cursor refers.
  *    @option options [Boolean] :nodupdata Delete all of the data
 *        items for the current key. This flag may only be specified
 *        if the database was opened with +:dupsort+.
 */
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

/**
 * @overload cursor_db
 *   @return [Database] the database which this cursor is iterating over.
 */
static VALUE cursor_db(VALUE self) {
        CURSOR(self, cursor);
        return cursor->db;
}

/**
 * @overload count
 *    Return count of duplicates for current key.  This call is only
 *    valid on databases that support sorted duplicate data items
 *    +:dupsort+.
 *    @return [Number] count of duplicates
 */
static VALUE cursor_count(VALUE self) {
        CURSOR(self, cursor);
        size_t count;
        check(mdb_cursor_count(cursor->cur, &count));
        return SIZET2NUM(count);
}

void Init_lmdb_ext() {
        VALUE mLMDB;

        /**
         * Document-module: LMDB
         *
         * The LMDB module presents a Ruby API to the OpenLDAP Lightning Memory-mapped Database (LMDB).
         * @see http://symas.com/mdb/
         */
        mLMDB = rb_define_module("LMDB");
        rb_define_const(mLMDB, "LIB_VERSION", rb_str_new2(MDB_VERSION_STRING));
        rb_define_singleton_method(mLMDB, "new", environment_new, -1);

#define VERSION_CONST(name) rb_define_const(mLMDB, "LIB_VERSION_"#name, INT2NUM(MDB_VERSION_##name));
        VERSION_CONST(MAJOR)
        VERSION_CONST(MINOR)
        VERSION_CONST(PATCH)
#undef VERSION_CONST

        /**
         * Document-class: LMDB::Error
         *
         * A general class of exceptions raised within the LMDB gem.
         */
        cError = rb_define_class_under(mLMDB, "Error", rb_eRuntimeError);
#define ERROR(name) cError_##name = rb_define_class_under(cError, #name, cError);
#include "errors.h"
#undef ERROR

        /**
         * Document-class: LMDB::Environment
         *
         * The Environment is the root object for all LMDB operations.
         *
         * An LMDB "environment" is a collection of one or more "databases"
         * (key-value tables), along with transactions to modify those
         * databases and cursors to iterate through them.
         *
         * An environment -- and its collection of databases -- is normally
         * stored in a directory.  That directory will contain two files:
         * * +data.mdb+: all the records in all the databases in the environment
         * * +lock.mdb+: state of transactions that may be going on in the environment.
         *
         * An environment can contain multiple databases.  Each of the
         * databases has a string name ("mydatabase", "db.3.1982").  You use
         * the database name to open the database within the environment.
         *
         * @example The normal pattern for using LMDB in Ruby
         *    env = LMDB.new "databasedir"
         *    db = env.database "databasename"
         *    # ... do things to the database ...
         *    env.close
         */
        cEnvironment = rb_define_class_under(mLMDB, "Environment", rb_cObject);
        rb_define_singleton_method(cEnvironment, "new", environment_new, -1);
        rb_define_method(cEnvironment, "database", environment_database, -1);
        rb_define_method(cEnvironment, "active_txn", environment_active_txn, 0);
        rb_define_method(cEnvironment, "close", environment_close, 0);
        rb_define_method(cEnvironment, "stat", environment_stat, 0);
        rb_define_method(cEnvironment, "info", environment_info, 0);
        rb_define_method(cEnvironment, "copy", environment_copy, 1);
        rb_define_method(cEnvironment, "sync", environment_sync, -1);
        rb_define_method(cEnvironment, "mapsize=", environment_set_mapsize, 1);
        rb_define_method(cEnvironment, "set_flags", environment_set_flags, -1);
        rb_define_method(cEnvironment, "clear_flags", environment_clear_flags, -1);
        rb_define_method(cEnvironment, "flags", environment_flags, 0);
        rb_define_method(cEnvironment, "path", environment_path, 0);
        rb_define_method(cEnvironment, "transaction", environment_transaction, -1);

        /**
         * Document-class: LMDB::Database
         *
         * An LMDB Database is a table of key-value pairs.  It is stored as
         * part of the {Environment}.
         *
         * By default, each key in a Database maps to one value.  However, a
         * Database can be configured at creation to allow duplicate keys, in
         * which case one key will map to multiple values.
         *
         * A Database stores the keys in a sorted order.  The order can also
         * be set with options when the database is created.
         *
         * The basic operations on a database are to {#put}, {#get}, and
         * {#delete} records.  One can also iterate through the records in a
         * database using a {Cursor}.
         *
         * @example Typical usage
         *    env = LMDB.new "databasedir"
         *    db = env.database "databasename"
         *    db.put "key1", "value1"
         *    db.put "key2", "value2"
         *    db.get "key1"              #=> "value1"
         *    env.close
         */
        cDatabase = rb_define_class_under(mLMDB, "Database", rb_cObject);
        rb_undef_method(rb_singleton_class(cDatabase), "new");
        rb_define_method(cDatabase, "stat", database_stat, 0);
        rb_define_method(cDatabase, "drop", database_drop, 0);
        rb_define_method(cDatabase, "clear", database_clear, 0);
        rb_define_method(cDatabase, "get", database_get, 1);
        rb_define_method(cDatabase, "put", database_put, -1);
        rb_define_method(cDatabase, "delete", database_delete, -1);
        rb_define_method(cDatabase, "cursor", database_cursor, 0);
        rb_define_method(cDatabase, "env", database_env, 0);

        /**
         * Document-class: LMDB::Transaction
         *
         * The LMDB environment supports transactional reads and updates.  By
         * default, these provide the standard ACID (atomicity, consistency,
         * isolation, durability) behaviors.
         *
         * Transactions can be committed or aborted.  When a transaction is
         * committed, all its effects take effect in the database atomically.
         * When a transaction is aborted, none of its effects take effect.
         *
         * Transactions span the entire environment.  All the updates made in
         * the course of an update transaction -- writing records across all
         * databases, creating databases, and destroying databases -- are
         * either completed atomically or rolled back.
         *
         * Transactions can be nested.  A child transaction can be started
         * within a parent transaction.  The child transaction can commit or
         * abort, at which point the effects of the child become visible to
         * the parent transaction or not.  If the parent aborts, all of the
         * changes performed in the context of the parent -- including the
         * changes from a committed child transaction -- are rolled back.
         *
         * To create a transaction, call {Environment#transaction} and supply
         * a block for the code to execute in that transaction.
         *
         * @example Typical usage
         *    env = LMDB.new "databasedir"
         *    db1 = env.database "database1"
         *    env.transaction do |parent|
         *      db2 = env.database "database2", :create => true
         *                            #=> creates a new database, but it isn't
         *                            #=> yet committed to storage
         *      db1['x']              #=> nil
         *      env.transaction do |child1|
         *        db2['a'] = 'b'
         *        db1['x'] = 'y'
         *      end
         *                            #=> first child transaction commits
         *                            #=> changes are visible within the parent transaction
         *                            #=> but are not yet permanent
         *      db1['x']              #=> 'y'
         *      db2['a']              #=> 'a'
         *      env.transaction do |child2|
         *        db2['a'] = 'def'
         *        db1['x'] = 'ghi'
         *        child2.abort
         *                            #=> second child transaction aborts and rolls
         *                            #=> back its changes
         *      end
         *      db1['x']              #=> 'y'
         *      db2['a']              #=> 'a'
         *    end
         *                            #=> parent transaction commits and writes database2
         *                            #=> and the updates from transaction child1 to
         *                            #=> storage.
         */
        cTransaction = rb_define_class_under(mLMDB, "Transaction", rb_cObject);
        rb_undef_method(rb_singleton_class(cTransaction), "new");
        rb_define_method(cTransaction, "commit", transaction_commit, 0);
        rb_define_method(cTransaction, "abort", transaction_abort, 0);
        rb_define_method(cTransaction, "env", transaction_env, 0);

        /**
         * Document-class: LMDB::Cursor
         *
         * A Cursor points to records in a database, and is used to iterate
         * through the records in the database.
         *
         * Cursors are created in the context of a transaction, and should
         * only be used as long as that transaction is active.  In other words,
         * after you {Transaction#commit} or {Transaction#abort} a transaction,
         * the cursors created while that transaction was active are no longer
         * usable.
         *
         * To create a cursor, call {Database#cursor} and pass it a block for
         * that should be performed using the cursor.
         *
         * @example Typical usage
         *    env = LMDB.new "databasedir"
         *    db = env.database "databasename"
         *    db.cursor do |cursor|
         *      rl = cursor.last           #=> content of the last record
         *      r1 = cursor.first          #=> content of the first record
         *      r2 = cursor.next           #=> content of the second record
         *      cursor.put "x", "y", current: true
         *                                 #=> replaces the second record with a new value "y"
         *    end
         */
        cCursor = rb_define_class_under(mLMDB, "Cursor", rb_cObject);
        rb_undef_method(rb_singleton_class(cCursor), "new");
        rb_define_method(cCursor, "close", cursor_close, 0);
        rb_define_method(cCursor, "get", cursor_get, 0);
        rb_define_method(cCursor, "first", cursor_first, 0);
        rb_define_method(cCursor, "last", cursor_last, 0);
        rb_define_method(cCursor, "next", cursor_next, 0);
        rb_define_method(cCursor, "next_range", cursor_next_range, 1);
        rb_define_method(cCursor, "prev", cursor_prev, 0);
        rb_define_method(cCursor, "set", cursor_set, 1);
        rb_define_method(cCursor, "set_range", cursor_set_range, 1);
        rb_define_method(cCursor, "put", cursor_put, -1);
        rb_define_method(cCursor, "count", cursor_count, 0);
        rb_define_method(cCursor, "delete", cursor_delete, -1);
        rb_define_method(cCursor, "database", cursor_db, 0);
}
