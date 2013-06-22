require 'lmdb/lmdb_ext'

module LMDB

  # @see LMDB::Environment#new
  def self.new(*a, &b)
    LMDB::Environment.new(*a, &b)
  end

end

%w|environment database|.each do |name|
  require "lmdb/#{name}"
end