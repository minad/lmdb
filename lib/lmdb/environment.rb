class LMDB::Environment

  DEFAULT_OPTS = {
    path: ".",
    mode: 0755,
    max_size: (2**28-1), # 4G
    # max_size: 1_000_000,
    max_dbs: 16,
    max_readers: 126,
    sub_dirs: true,
    read_only: false,
    mmap: false,
    sync: true,
    meta_sync: false
  }.freeze

  INFO_MAP = {
    mapaddr:    :address,
    mapsize:    :max_size,
    last_pgno:  :last_page,
    last_txnid: :last_transaction_id,
    maxreaders: :maxreaders,
    numreaders: :readers
  }.freeze

  STAT_MAP = {
    psize:          :page_size,
    depth:          :depth,
    branch_pages:   :branch_pages,
    leaf_pages:     :leaf_pages,
    overflow_pages: :overflow_pages,
    entries:        :entries
  }.freeze

  # @attr_reader [String] path environment path
  attr_reader :path

  # Constructor, opens an environment
  #
  # @param [Hash] opts options
  # @option opts [String] :path
  #   the environment path, defaults to current path
  # @option opts [String] :mode
  #   the file mode, defaults to 0755
  # @option opts [Integer] :max_size
  #   the maximum environment size in bytes, defaults to 4G
  # @option opts [Integer] :max_dbs
  #   the maximum number of databases, defaults to 16
  # @option opts [Integer] :max_readers
  #   the maximum number of readers, defaults to 126
  # @option opts [Boolean] :sub_dirs
  #   allow subdirectories, defaults to true
  # @option opts [Boolean] :read_only
  #   open in read-only mode, defaults to false
  # @option opts [Boolean] :mmap
  #   use memory map, faster, but less durable, incompatible with nested transactions, defaults to false
  # @option opts [Boolean] :sync
  #   flushes system buffers at the end of a transaction; slower, but more durable, defaults to true
  # @option opts [Boolean] :meta_sync
  #   flush system buffers on every write and not only at the end of a transaction; slower, but more durable, defaults to false
  # @raises [LMDB::Error] on errors
  def initialize(opts = {})
    opts  = DEFAULT_OPTS.merge(opts)
    @path = opts[:path].to_s
    @raw  = LMDB::Ext::Environment.open path, parse(opts)
  end

  # Manually sync the environment
  #
  # @param [Hash] opts options
  # @option opts [Boolean] :force force sync, default false
  # @return [Boolean] true if successful
  # @raises [LMDB::Error] on errors
  def sync(opts = {})
    @raw.sync !!opts[:force]
    true
  end

  # @return [Hash] environment info & stats
  def info
    info, stat = @raw.info, @raw.stat
    hash = {}
    INFO_MAP.each {|m, f| hash[f] = info.send(m) }
    STAT_MAP.each {|m, f| hash[f] = stat.send(m) }
    hash
  end

  # TODO: Thread-safe global transactions
  #
  # Executes a transaction
  # @yield a transaction
  # @yieldparam [LMDB::Transaction] txn the transaction instance
  # def transaction(&block)
  #   LMDB::Transaction.send(:new, self, &block)
  # end

  # Opens a database
  # @param [String] name
  # @return [LMDB::Database] the database
  def database(name)
    LMDB::Database.send(:new, self, name)
  end
  alias_method :db, :database

  # @return [String] introspection
  def inspect
    "#<#{self.class.name} @path=#{path.inspect}>"
  end

  private

    def raw
      @raw
    end

    def rtxn(&block)
      @raw.transaction(&block)
    end

    def parse(opts)
      flags = 0
      flags |= LMDB::NOSUBDIR   unless opts[:sub_dirs]
      flags |= LMDB::RDONLY     if opts[:read_only]
      flags |= LMDB::WRITEMAP   if opts[:mmap]
      flags |= LMDB::NOSYNC     unless opts[:sync]
      flags |= LMDB::NOMETASYNC unless opts[:meta_sync]

      { flags: flags,
        mode: opts[:mode],
        maxreaders: opts[:max_readers].to_i,
        mapsize: opts[:max_size].to_i,
        maxdbs: opts[:max_dbs].to_i }
    end

end
