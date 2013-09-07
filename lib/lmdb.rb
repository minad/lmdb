require 'lmdb_ext'

module LMDB
  class Database
    include Enumerable

    def each
      cursor {|c| yield(c.next) }
    end

    def [](key)
      get(key)
    end

    def []=(key, value)
      put(key, value)
      value
    end

    def size
      stat[:entries]
    end
  end
end

