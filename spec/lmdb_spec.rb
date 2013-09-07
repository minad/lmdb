require 'helper'

describe LMDB do
  let(:env) { LMDB::Environment.open(path) }
  after     { env.close rescue nil }

  let(:txn) { env.transaction }
  after     { txn.abort rescue nil }

  let(:db)  { txn.open('db', LMDB::CREATE) }
  after     { db.close rescue nil }

  it 'has version constants' do
    LMDB::VERSION_MAJOR.should be_instance_of(Fixnum)
    LMDB::VERSION_MINOR.should be_instance_of(Fixnum)
    LMDB::VERSION_PATCH.should be_instance_of(Fixnum)
    LMDB::VERSION.should be_instance_of(String)
  end

  it 'has environment flags' do
    LMDB::FIXEDMAP.should be_instance_of(Fixnum)
    LMDB::NOSUBDIR.should be_instance_of(Fixnum)
    LMDB::NOSYNC.should be_instance_of(Fixnum)
    LMDB::RDONLY.should be_instance_of(Fixnum)
    LMDB::NOMETASYNC.should be_instance_of(Fixnum)
    LMDB::WRITEMAP.should be_instance_of(Fixnum)
    LMDB::MAPASYNC.should be_instance_of(Fixnum)
  end

  it 'has database flags' do
    LMDB::REVERSEKEY.should be_instance_of(Fixnum)
    LMDB::DUPSORT.should be_instance_of(Fixnum)
    LMDB::INTEGERKEY.should be_instance_of(Fixnum)
    LMDB::DUPFIXED.should be_instance_of(Fixnum)
    LMDB::INTEGERDUP.should be_instance_of(Fixnum)
    LMDB::REVERSEDUP.should be_instance_of(Fixnum)
    LMDB::CREATE.should be_instance_of(Fixnum)
    LMDB::NOOVERWRITE.should be_instance_of(Fixnum)
    LMDB::NODUPDATA.should be_instance_of(Fixnum)
    LMDB::CURRENT.should be_instance_of(Fixnum)
    LMDB::RESERVE.should be_instance_of(Fixnum)
    LMDB::APPEND.should be_instance_of(Fixnum)
    LMDB::APPENDDUP.should be_instance_of(Fixnum)
    LMDB::MULTIPLE.should be_instance_of(Fixnum)
  end

  describe LMDB::Stat do
    subject { env.stat }

    it { should be_instance_of(described_class::Stat) }
    its(:psize) { should be_instance_of(Fixnum) }
    its(:depth) { should be_instance_of(Fixnum) }
    its(:branch_pages) { should be_instance_of(Fixnum) }
    its(:leaf_pages) { should be_instance_of(Fixnum) }
    its(:overflow_pages) { should be_instance_of(Fixnum) }
    its(:entries) { should be_instance_of(Fixnum) }
  end

  describe LMDB::Info do
    subject { env.info }

    it { should be_instance_of(described_class::Info) }
    its(:mapaddr) { should be_instance_of(Fixnum) }
    its(:mapsize) { should be_instance_of(Fixnum) }
    its(:last_pgno) { should be_instance_of(Fixnum) }
    its(:last_txnid) { should be_instance_of(Fixnum) }
    its(:maxreaders) { should be_instance_of(Fixnum) }
    its(:numreaders) { should be_instance_of(Fixnum) }
  end

  describe LMDB::Environment do
    subject { env }

    its(:path)  { should == path }
    its(:flags) { should == 0 }

    describe 'open' do
      it 'returns environment' do
        env = LMDB::Environment.open(path)
        env.should be_instance_of(described_class::Environment)
        env.close
      end

      it 'accepts block' do
        LMDB::Environment.open(path) do |env|
          env.should be_instance_of(described_class::Environment)
          42
        end.should == 42
      end

      it 'accepts options' do
        env = LMDB::Environment.open(path, :flags => LMDB::NOSYNC, :mode => 0777, :maxreaders => 777, :mapsize => 111111, :maxdbs => 666)
        env.should be_instance_of(described_class::Environment)
        env.info.maxreaders.should == 777
        env.info.mapsize.should == 111111
        env.close
      end
    end

    it 'should copy' do
      target = mkpath('copy')
      subject.copy(target).should be_nil
    end

    it 'should sync' do
      subject.sync.should be_nil
    end

    it 'should force-sync' do
      subject.sync(true).should be_nil
    end

    it 'should accept custom flags' do
      (subject.flags = LMDB::NOSYNC).should == LMDB::NOSYNC
      subject.flags.should == LMDB::NOSYNC

      (subject.flags = 0).should == 0
      subject.flags.should == 0
    end

    it 'should create transactions' do
      ctxn = subject.transaction
      ctxn.should be_instance_of(described_class::Transaction)
      ctxn.parent.should be_nil
      ctxn.environment.should == subject
      ctxn.abort
    end

    it 'should create read-only transactions' do
      ctxn = subject.transaction(true)
      ctxn.should be_instance_of(described_class::Transaction)
      ctxn.abort
    end

    it 'should create transactions blocks' do
      subject.transaction do |ctxn|
        ctxn.should be_instance_of(described_class::Transaction)
        ctxn.environment.should == subject
        ctxn.parent.should be_nil
      end
    end
  end

  describe LMDB::Transaction do
    subject { txn }

    its(:environment) { should == env }
    its(:parent) { should be_nil }

    it 'can create child transactions' do
      ctxn = subject.transaction
      ctxn.should be_instance_of(described_class::Transaction)
      ctxn.parent.should == subject
      ctxn.environment.should == env
      ctxn.abort
    end

    it 'can create block transactions' do
      subject.transaction do |ctxn|
        ctxn.should be_instance_of(described_class::Transaction)
        ctxn.parent.should == subject
      end
    end
  end

  describe LMDB::Database do
    subject { db }

    it 'should get/set data' do
      subject.get('cat').should be_nil
      subject.set('cat', 'garfield').should be_nil
      subject.get('cat').should == 'garfield'
    end

    it 'should get/set data' do
      subject.get('cat').should be_nil
      subject.set('cat', 'garfield').should be_nil
      subject.get('cat').should == 'garfield'
    end

    it 'stores key/values in same transaction' do
      db.put('key', 'value').should be_nil
      db.get('key').should == 'value'
    end

    it 'stores key/values in different transactions' do
      txn.transaction do |ctxn|
        db.put('key', 'value').should be_nil
      end

      txn.transaction do |ctxn|
        db.get('key').should == 'value'
      end
    end

    it 'should return stat' do
      db.stat.should be_instance_of(described_class::Stat)
    end

    it 'should close' do
      db.close.should be_nil
      proc { db.close }.should raise_error(LMDB::Error, /closed/)
    end
  end

  describe LMDB::Cursor do
    before do
      db.put('key1', 'value1')
      db.put('key2', 'value2')
    end

    it 'should get next key/value' do
      db.cursor do |c|
        c.first.should == ['key1', 'value1']
      end
    end

    it 'should get next key/value' do
      db.cursor do |c|
        c.first
        c.next.should == ['key2', 'value2']
      end
    end

    it 'should seek to key' do
      db.cursor do |c|
        c.set('key1').should == ['key1', 'value1']
      end
    end

    it 'should seek to closest key' do
      db.cursor do |c|
        c.set_range('key0').should == ['key1', 'value1']
      end
    end

    it 'should seek to key with nuls' do
      db.cursor do |c|
        c.set_range('\x00').should == ['key1', 'value1']
      end
    end
  end
end
