require 'lmdb_ext'

module LMDB
  class Database
    include Enumerable

    def each
      cursor {|c| yield(c.next) }
    end
  end
end

