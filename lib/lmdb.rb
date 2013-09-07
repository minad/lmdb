require 'lmdb_ext'

module LMDB
  class Database
    include Enumerable

    def each
      cursor do |c|
        while i = c.next
          yield(i)
        end
      end
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

