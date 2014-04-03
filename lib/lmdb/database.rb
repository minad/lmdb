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
      cursor do |c|
        while i = c.next
          yield(i)
        end
      end
    end

    # Retrieve the value of a record from a database
    # @param key the record key to retrieve
    # @return value of the record for that key, or nil if there is
    #      no record with that key
    # @see #get(key)
    def [](key)
      value = get(key)
      if value && value[0] == "\x04"
        load_serialized(value)
      else
        value
      end
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
      if value.is_a? String
        put(key, value)
      else
        serialized_value = Marshal.dump(value)
        put(key, serialized_value)
      end
      value
    end

    # @return the number of records in this database
    def size
      stat[:entries]
    end

    private
      def load_serialized(value)
        Marshal.load(value)
      rescue TypeError
        value
      end
  end
end
