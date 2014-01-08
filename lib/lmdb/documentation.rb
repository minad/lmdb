# The LMDB module presents a Ruby API to the OpenLDAP Lightning Memory-mapped Database (LMDB).
# @see http://symas.com/mdb/
module LMDB

  # The Environment is the root object for all LMDB operations.  
  # 
  # An LMDB "environment" is a collection of one or more "databases"
  # (key-value tables), along with transactions to modify those
  # databases and cursors to iterate through them. 
  #
  # An environment -- and its collection of databases -- is normally
  # stored in a directory.  That directory will contain two files:
  # * +data.mdb+: all the records in all the databases in the environment
  # * +lock.mdb+: state of transactions that may be going on in the environment.
  #
  # An environment can contain multiple databases.  Each of the
  # databases has a string name ("mydatabase", "db.3.1982").  You use
  # the database name to open the database within the environment.
  #
  # The normal pattern for using LMDB in Ruby looks something like:
  #
  #    env = Environment.new "databasedir"
  #    db = env.database "databasename"
  #    # ... do things to the database ...
  #    env.close
  class Environment
  end

  # An LMDB Database is a table of key-value pairs.  It is stored as 
  # part of the {Environment}.
  #
  # By default, each key in a Database maps to one value.  However, a
  # Database can be configured at creation to allow duplicate keys, in
  # which case one key will map to multiple values.
  #
  # A Database stores the keys in a sorted order.  The order can also
  # be set with options when the database is created.
  #
  # The basic operations on a database are to {#put}, {#get}, and
  # {#delete} records.  One can also iterate through the records in a
  # database using a {Cursor}.
  #
  # Typical usage might be:
  #    
  #    env = Environment.new "databasedir"
  #    db = env.database "databasename"
  #    db.put "key1", "value1"
  #    db.put "key2", "value2"
  #    db.get "key1"              #=> "value1"
  #    env.close
  class Database
  end

  # A Cursor points to records in a database, and is used to iterate
  # through the records in the database.
  #
  # Cursors are created in the context of a transaction, and should
  # only be used as long as that transaction is active.  In other words,
  # after you {Transaction#commit} or {Transaction#abort} a transaction,
  # the cursors created while that transaction was active are no longer 
  # usable.
  #
  # To create a cursor, call {Database#cursor} and pass it a block for
  # that should be performed using the cursor.  
  #
  # Typical usage might be:
  #
  #    env = Environment.new "databasedir"
  #    db = env.database "databasename"
  #    db.cursor do |cursor|
  #      rl = cursor.last           #=> content of the last record
  #      r1 = cursor.first          #=> content of the first record 
  #      r2 = cursor.next           #=> content of the second record
  #      cursor.put "x", "y", current: true
  #                                 #=> replaces the second record with a new value "y"
  #    end
  class Cursor
  end

  # The LMDB environment supports transactional reads and updates.  By 
  # default, these provide the standard ACID (atomicity, consistency, 
  # isolation, durability) behaviors.
  #
  # Transactions can be committed or aborted.  When a transaction is
  # committed, all its effects take effect in the database atomically.
  # When a transaction is aborted, none of its effects take effect.
  #
  # Transactions span the entire environment.  All the updates made in
  # the course of an update transaction -- writing records across all
  # databases, creating databases, and destroying databases -- are
  # either completed atomically or rolled back.
  #
  # Transactions can be nested.  A child transaction can be started
  # within a parent transaction.  The child transaction can commit or
  # abort, at which point the effects of the child become visible to
  # the parent transaction or not.  If the parent aborts, all of the
  # changes performed in the context of the parent -- including the
  # changes from a committed child transaction -- are rolled back.
  #
  # To create a transaction, call {Environment#transaction} and supply
  # a block for the code to execute in that transaction.
  #
  # Typical usage might be:
  #    env = Environment.new "databasedir"
  #    db1 = env.database "database1"
  #    env.transaction do |parent|
  #      db2 = env.database "database2", :create => true
  #                            #=> creates a new database, but it isn't
  #                            #=> yet committed to storage
  #      db1['x']              #=> nil
  #      env.transaction do |child1|
  #        db2['a'] = 'b'
  #        db1['x'] = 'y'
  #      end
  #                            #=> first child transaction commits
  #                            #=> changes are visible within the parent transaction 
  #                            #=> but are not yet permanent
  #      db1['x']              #=> 'y'
  #      db2['a']              #=> 'a'
  #      env.transaction do |child2|
  #        db2['a'] = 'def'
  #        db1['x'] = 'ghi'
  #        child2.abort
  #                            #=> second child transaction aborts and rolls
  #                            #=> back its changes
  #      end
  #      db1['x']              #=> 'y'
  #      db2['a']              #=> 'a'
  #    end
  #                            #=> parent transaction commits and writes database2
  #                            #=> and the updates from transaction child1 to
  #                            #=> storage.
  class Transaction
  end

  # A general class of exceptions raised within the LMDB gem.
  class Error
  end
  
end
