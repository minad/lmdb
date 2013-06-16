require 'mdb/mdb_ext'

module MDB

  # @see MDB::Environment#new
  def self.new(*a, &b)
    MDB::Environment.new(*a, &b)
  end

end

%w|environment database|.each do |name|
  require "mdb/#{name}"
end