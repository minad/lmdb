require './ext/mdb'
require 'fileutils'
require 'minitest/spec'
require 'minitest/autorun'

OpenSpecs = proc do
  it 'returns Environment' do
    env = subject.open(mkpath)
    env.must_be_instance_of MDB::Environment
    env.close
  end

  it 'accepts block' do
    subject.open(mkpath) do |env|
      env.must_be_instance_of MDB::Environment
      42
    end.must_equal 42
  end

  it 'accepts options' do
    env = subject.open(mkpath, :flags => MDB::NOSYNC, :mode => 0777, :maxreaders => 777, :mapsize => 111111, :maxdbs => 666)
    env.must_be_instance_of MDB::Environment
    env.info.maxreaders.must_equal 777
    env.info.mapsize.must_equal 111111
    env.close
  end
end

describe MDB do
  def mkpath(name = 'env')
    path = File.join(File.dirname(__FILE__), 'tmp', name)
    FileUtils.mkpath path
    path
  end

  before do
    @env = MDB.open(mkpath)
  end

  after do
    @env.close
    FileUtils.rm_rf(File.join(File.dirname(__FILE__), 'tmp'))
  end

  it 'has version constants' do
    MDB::VERSION_MAJOR.must_be_instance_of Fixnum
    MDB::VERSION_MINOR.must_be_instance_of Fixnum
    MDB::VERSION_PATCH.must_be_instance_of Fixnum
    MDB::VERSION.must_be_instance_of String
  end

  it 'has environment flags' do
    MDB::FIXEDMAP.must_be_instance_of Fixnum
    MDB::NOSUBDIR.must_be_instance_of Fixnum
    MDB::NOSYNC.must_be_instance_of Fixnum
    MDB::RDONLY.must_be_instance_of Fixnum
    MDB::NOMETASYNC.must_be_instance_of Fixnum
    MDB::WRITEMAP.must_be_instance_of Fixnum
    MDB::MAPASYNC.must_be_instance_of Fixnum
  end

  it 'has database flags' do
    MDB::REVERSEKEY.must_be_instance_of Fixnum
    MDB::DUPSORT.must_be_instance_of Fixnum
    MDB::INTEGERKEY.must_be_instance_of Fixnum
    MDB::DUPFIXED.must_be_instance_of Fixnum
    MDB::INTEGERDUP.must_be_instance_of Fixnum
    MDB::REVERSEDUP.must_be_instance_of Fixnum
    MDB::CREATE.must_be_instance_of Fixnum
    MDB::NOOVERWRITE.must_be_instance_of Fixnum
    MDB::NODUPDATA.must_be_instance_of Fixnum
    MDB::CURRENT.must_be_instance_of Fixnum
    MDB::RESERVE.must_be_instance_of Fixnum
    MDB::APPEND.must_be_instance_of Fixnum
    MDB::APPENDDUP.must_be_instance_of Fixnum
    MDB::MULTIPLE.must_be_instance_of Fixnum
  end

  describe '#open' do
    subject { MDB }
    instance_eval(&OpenSpecs)
  end

  describe MDB::Stat do
    subject { @env.stat }

    it 'is returned by Environment#stat' do
      subject.must_be_instance_of MDB::Stat
    end

    it 'has attributes' do
      subject.psize.must_be_instance_of Fixnum
      subject.depth.must_be_instance_of Fixnum
      subject.branch_pages.must_be_instance_of Fixnum
      subject.leaf_pages.must_be_instance_of Fixnum
      subject.overflow_pages.must_be_instance_of Fixnum
      subject.entries.must_be_instance_of Fixnum
    end
  end

  describe MDB::Info do
    subject { @env.info }

    it 'is returned by Environment#info' do
      subject.must_be_instance_of MDB::Info
    end

    it 'has attributes' do
      subject.mapaddr.must_be_instance_of Fixnum
      subject.mapsize.must_be_instance_of Fixnum
      subject.last_pgno.must_be_instance_of Fixnum
      subject.last_txnid.must_be_instance_of Fixnum
      subject.maxreaders.must_be_instance_of Fixnum
      subject.numreaders.must_be_instance_of Fixnum
    end
  end

  describe MDB::Environment do
    subject { @env }

    describe '#open' do
      subject { MDB::Environment }
      instance_eval(&OpenSpecs)
    end

    describe '#copy' do
      it 'copies environment' do
        subject.copy(mkpath('copy')).must_equal nil
      end
    end

    describe '#sync' do
      it 'syncs environment' do
        subject.sync.must_equal nil
      end

      it 'accepts force argument' do
        subject.sync(true).must_equal nil
      end
    end

    describe '#path' do
      it 'has path' do
        subject.path.must_equal mkpath
      end
    end

    describe 'attribute flags' do
      it 'has flags' do
        subject.flags.must_equal 0
      end

      it 'sets flags' do
        (subject.flags = MDB::NOSYNC).must_equal MDB::NOSYNC
        subject.flags.must_equal MDB::NOSYNC

        (subject.flags = 0).must_equal 0
        subject.flags.must_equal 0
      end
    end

    describe '#transaction' do
      it 'returns MDB::Transaction' do
        txn = subject.transaction
        txn.must_be_instance_of MDB::Transaction
        txn.parent.must_equal nil
        txn.environment.must_equal subject
        txn.abort
      end

      it 'accepts read only argument' do
        txn = subject.transaction(true)
        txn.must_be_instance_of MDB::Transaction
        txn.abort
      end

      it 'accepts block' do
        subject.transaction do |txn|
          txn.must_be_instance_of MDB::Transaction
        txn.environment.must_equal subject
          txn.parent.must_equal nil
        end
      end
    end
  end

  describe MDB::Transaction do
    subject { @env.transaction }

    after do
      subject.abort rescue nil
    end

    it 'has #environment' do
      subject.environment.must_equal @env
    end

    it 'has #abort' do
      subject.abort.must_equal nil
    end

    it 'has #commit' do
      subject.commit.must_equal nil
    end

    it 'has #parent' do
      subject.parent.must_equal nil
    end

    describe '#transaction' do
      it 'creates child transaction' do
        txn = subject.transaction
        txn.must_be_instance_of MDB::Transaction
        txn.parent.must_equal subject
        txn.environment.must_equal subject.environment
        txn.abort
      end

      it 'accepts block' do
        subject.transaction do |txn|
          txn.must_be_instance_of MDB::Transaction
          txn.parent.must_equal subject
        end
      end
    end
  end

  describe MDB::Database do
    subject do
      @env.transaction do |txn|
        @env.open(txn, 'db', MDB::CREATE)
      end
    end

    after do
      subject.close
    end

    it 'stores key/values in same transaction' do
      db = subject
      @env.transaction do |txn|
        db.put(txn, 'key', 'value').must_equal nil
        db.get(txn, 'key').must_equal 'value'
      end
    end

    it 'stores key/values in different transactions' do
      db = subject
      @env.transaction do |txn|
        db.put(txn, 'key', 'value').must_equal nil
      end

      @env.transaction do |txn|
        db.get(txn, 'key').must_equal 'value'
      end
    end
  end

  describe MDB::Cursor do
  end
end
