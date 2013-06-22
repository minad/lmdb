class LMDB::Database

  class << self
    private :new
  end

  # @attr_reader [String] name
  attr_reader :name

  # @attr_reader [LMDB::Environment] env
  attr_reader :env

  # @param [LMDB::Environment] env
  # @param [String] name the DB name
  def initialize(env, name)
    @env  = env
    @name = name.to_s
  end

  # Closes the DB, doesn't fail if already closed
  def close
    raw.close if loaded?
  end

  # Reads a key
  # @param [String] key
  def get(key)
    ensure_db!
    rtxn {|txn| raw.get(txn, key) }
  rescue LMDB::Ext::Error::NOTFOUND
  end
  alias_method :[], :get

  # Sets a key to a new value
  # @param [String] key
  # @param [String] value
  def set(key, value)
    ensure_db!
    rtxn {|txn| raw.put(txn, key, value) }
  end
  alias_method :put, :set
  alias_method :[]=, :set

  private

    def raw
      @raw ||= rtxn {|txn| renv.open(txn, name, LMDB::CREATE) }
    end

    def renv
      @renv ||= env.send(:raw)
    end

    def rtxn(&block)
      env.send(:rtxn, &block)
    end

    def loaded?
      !!@raw
    end

    def ensure_db!
      raw unless loaded?
    end

end
