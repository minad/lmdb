# coding: binary
require 'helper'

describe LMDB do
  let(:env) { LMDB.open(path) }
  after     { env.close rescue nil }

  let(:db)  { env.database }

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
    LMDB::NOTLS.should be_instance_of(Fixnum)
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

  describe LMDB::Environment do
    subject { env }

    it 'should return flags' do
      subject.flags.should be_instance_of(Fixnum)
    end

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
        env.info[:maxreaders].should == 777
        env.info[:mapsize].should == 111111
        env.close
      end
    end

    it 'should return stat' do
      stat = env.stat
      stat[:psize].should be_instance_of(Fixnum)
      stat[:depth].should be_instance_of(Fixnum)
      stat[:branch_pages].should be_instance_of(Fixnum)
      stat[:leaf_pages].should be_instance_of(Fixnum)
      stat[:overflow_pages].should be_instance_of(Fixnum)
      stat[:entries].should be_instance_of(Fixnum)
    end

    it 'should return info' do
      info = env.info
      info[:mapaddr].should be_instance_of(Fixnum)
      info[:mapsize].should be_instance_of(Fixnum)
      info[:last_pgno].should be_instance_of(Fixnum)
      info[:last_txnid].should be_instance_of(Fixnum)
      info[:maxreaders].should be_instance_of(Fixnum)
      info[:numreaders].should be_instance_of(Fixnum)
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
      (subject.flags & LMDB::NOSYNC).should == 0

      subject.set_flags LMDB::NOSYNC
      (subject.flags & LMDB::NOSYNC).should == LMDB::NOSYNC

      subject.clear_flags LMDB::NOSYNC
      (subject.flags & LMDB::NOSYNC).should == 0
    end

    describe 'transaction' do
      subject { env}

      it 'should create transactions' do
        subject.active_txn.should == nil
        subject.transaction do |txn|
          subject.active_txn.should == txn
          txn.should be_instance_of(described_class::Transaction)
          txn.abort
          subject.active_txn.should == nil
        end
        subject.active_txn.should == nil
      end

      it 'should create read-only transactions' do
        subject.active_txn.should == nil
        subject.transaction(true) do |txn|
          subject.active_txn.should == txn
          txn.should be_instance_of(described_class::Transaction)
          txn.abort
          subject.active_txn.should == nil
        end
        subject.active_txn.should == nil
      end

      it 'can create child transactions' do
        subject.active_txn.should == nil
        env.transaction do |txn|
          subject.active_txn.should == txn
          env.transaction do |ctxn|
            subject.active_txn.should == ctxn
            ctxn.abort
            subject.active_txn.should == txn
          end
          subject.active_txn.should == txn
        end
        subject.active_txn.should == nil
      end

      it 'should support aborting parent transaction' do
        subject.active_txn.should == nil
        env.transaction do |txn|
          subject.active_txn.should == txn
          env.transaction do |ctxn|
            subject.active_txn.should == ctxn
            db['key'] = 'value'
            txn.abort
            subject.active_txn.should == nil
          end
          subject.active_txn.should == nil
        end
        db['key'].should be(nil)
        subject.active_txn.should == nil
      end

      it 'should support comitting parent transaction' do
        subject.active_txn.should == nil
        env.transaction do |txn|
          subject.active_txn.should == txn
          env.transaction do |ctxn|
            subject.active_txn.should == ctxn
            db['key'] = 'value'
            txn.commit
            subject.active_txn.should == nil
          end
          subject.active_txn.should == nil
        end
        db['key'].should == 'value'
        subject.active_txn.should == nil
      end
    end
  end

  describe LMDB::Database do
    subject { db }

    it 'should support named databases' do
      main = env.database
      db1 = env.database('db1', LMDB::CREATE)
      db2 = env.database('db2', LMDB::CREATE)

      main['key'] = '1'
      db1['key'] = '2'
      db2['key'] = '3'

      main['key'].should == '1'
      db1['key'].should == '2'
      db2['key'].should == '3'
    end

    it 'should get/put data' do
      subject.get('cat').should be_nil
      subject.put('cat', 'garfield').should be_nil
      subject.get('cat').should == 'garfield'
    end

    it 'stores key/values in same transaction' do
      db.put('key', 'value').should be_nil
      db.get('key').should == 'value'
    end

    it 'stores key/values in different transactions' do
      env.transaction do
        db.put('key', 'value').should be_nil
        db.put('key2', 'value2').should be_nil
        env.transaction do
          db.put('key3', 'value3').should be_nil
        end
      end

      env.transaction do
        db.get('key').should == 'value'
        db.get('key2').should == 'value2'
        env.transaction do
          db.get('key3').should == 'value3'
        end
      end
    end

    it 'should return stat' do
      db.stat.should be_instance_of(Hash)
    end

    it 'should return size' do
      db.size.should == 0
      db.put('key', 'value')
      db.size.should == 1
      db.put('key2', 'value2')
      db.size.should == 2
    end

    it 'should be enumerable' do
      db['k1'] = 'v1'
      db['k2'] = 'v2'
      db.to_a.should == [['k1', 'v1'], ['k2', 'v2']]
    end

    it 'should have shortcuts' do
      db['key'] = 'value'
      db['key'].should == 'value'
    end

    it 'should store binary' do
      bin1 = "\xAAx\BB\xCC1"
      bin2 = "\xAAx\BB\xCC2"
      db[bin1] = bin2
      db['key'] = bin2
      db[bin1].should == bin2
      db['key'].should == bin2
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
