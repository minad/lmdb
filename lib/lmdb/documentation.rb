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
  
end
