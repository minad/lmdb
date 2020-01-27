module LMDB
  class Database
    include Enumerable

    # Iterate through the records in a database
    # @yield [i] Gives a record [key, value] to the block
    # @yieldparam [Array] i The key, value pair for each record
    # @example
    #    db.each do |record|
    #      key, value = record
    #      puts "at #{key}: #{value}"
    #    end
    def each
      env.transaction true do
        cursor do |c|
          while i = c.next
            yield(i)
          end
        end
      end
    end

    # Retrieve the value of a record from a database
    # @param key the record key to retrieve
    # @return value of the record for that key, or nil if there is
    #      no record with that key
    # @see #get(key)
    def [](key)
      get(key)
    end

    # Set (write or update) a record in a database.
    # @param key key for the record
    # @param value the value of the record
    # @return returns the value of the record
    # @see #put(key, value)
    # @example
    #      db['a'] = 'b'     #=> 'b'
    #      db['b'] = 1234    #=> 1234
    #      db['a']           #=> 'b'
    def []=(key, value)
      put(key, value)
      value
    end

    # Get the keys as an array.
    # @return [Array] of keys.
    def keys
      each_key.to_a
    end

    # Iterate over each key in the database, skipping over duplicate records.
    #
    # @yield key [String] the next key in the database.
    # @return [Enumerator] in lieu of a block.
    def each_key &block
      return enum_for :each_key unless block_given?
      env.transaction true do
        cursor do |c|
          while (k, _ = c.next true)
            yield k
          end
        end
      end
    end

    # Iterate over the duplicate values of a given key, using an
    # implicit cursor. Works whether +:dupsort+ is set or not.
    #
    # @param key [#to_s] The key in question.
    # @yield value [String] the next value associated with the key.
    # @return [Enumerator] in lieu of a block.
    def each_value key, &block
      return enum_for :each_value, key unless block_given?

      value = get(key) or return
      unless dupsort?
        yield value
        return
      end

      env.transaction true do
        cursor do |c|
          method = :set
          while rec = c.send(method, key)
            method = :next_range
            yield rec[1]
          end
        end
      end
    end

    # Return the cardinality (number of duplicates) of a given
    # key. Works whether +:dupsort+ is set or not.
    # @param key [#to_s] The key in question.
    # @return [Integer] The number of entries under the key.
    def cardinality key
      env.transaction true do
        return 0 unless get key
        return 1 unless dupsort?
        cursor do |c|
          c.set key
          return c.count
        end
      end
    end

    # Test if the database has a given key (or, if opened in
    # +:dupsort+, value)
    def has? key, value = nil
      v = get(key) or return false
      return true if value.nil? or value.to_s == v
      return false unless dupsort?

      env.transaction true do
        cursor do |c|
          return !!c.set(key, value)
        end
      end
    end

    # Delete the key (and optional value pair) if it exists; do not
    # complain about missing keys.
    # @param key [#to_s] The key.
    # @param value [#to_s] The optional value.
    def delete? key, value = nil
      delete key, value if has? key, value
    end

    # @return the number of records in this database
    def size
      stat[:entries]
    end
  end
end
