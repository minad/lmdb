# coding: binary
require 'helper'

describe LMDB do
  let(:env) { LMDB.new(path) }
  after     { env.close rescue nil }

  let(:db)  { env.database }

  it 'has version constants' do
    LMDB::LIB_VERSION_MAJOR.should be_instance_of(Integer)
    LMDB::LIB_VERSION_MINOR.should be_instance_of(Integer)
    LMDB::LIB_VERSION_PATCH.should be_instance_of(Integer)
    LMDB::LIB_VERSION.should be_instance_of(String)
    LMDB::VERSION.should be_instance_of(String)
  end

  describe LMDB::Environment do
    subject { env }

    it 'should return flags' do
      subject.flags.should be_instance_of(Array)
    end

    describe 'new' do
      it 'returns environment' do
        env = LMDB::Environment.new(path)
        env.should be_instance_of(described_class)
        env.close
      end

      it 'accepts block' do
        LMDB::Environment.new(path) do |env|
          env.should be_instance_of(described_class)
          42
        end.should == 42
      end

      it 'accepts options' do
        env = LMDB::Environment.new(path, :nosync => true, :mode => 0777, :maxreaders => 777, :mapsize => 111111, :maxdbs => 666)
        env.should be_instance_of(described_class)
        env.info[:maxreaders].should == 777
        env.info[:mapsize].should == 111111
        env.flags.should include(:nosync)
        env.close

        env = LMDB::Environment.new(path, :nosync => false)
        env.flags.should_not include(:nosync)
        env.close
      end
    end

    it 'should return stat' do
      stat = env.stat
      stat[:psize].should be_instance_of(Integer)
      stat[:depth].should be_instance_of(Integer)
      stat[:branch_pages].should be_instance_of(Integer)
      stat[:leaf_pages].should be_instance_of(Integer)
      stat[:overflow_pages].should be_instance_of(Integer)
      stat[:entries].should be_instance_of(Integer)
    end

    it 'should return info' do
      info = env.info
      info[:mapaddr].should be_instance_of(Integer)
      info[:mapsize].should be_instance_of(Integer)
      info[:last_pgno].should be_instance_of(Integer)
      info[:last_txnid].should be_instance_of(Integer)
      info[:maxreaders].should be_instance_of(Integer)
      info[:numreaders].should be_instance_of(Integer)
    end

    it 'should set mapsize' do
      size_before = env.info[:mapsize]
      env.mapsize = size_before * 2
      env.info[:mapsize].should == size_before * 2
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
      subject.flags.should_not include(:nosync)

      subject.set_flags :nosync
      subject.flags.should include(:nosync)

      subject.clear_flags :nosync
      subject.flags.should_not include(:nosync)
    end

    describe LMDB::Transaction do
      subject { env }

      it 'should create transactions' do
        subject.active_txn.should == nil
        subject.transaction do |txn|
          subject.active_txn.should == txn
          txn.should be_instance_of(described_class)
          txn.abort
          subject.active_txn.should == nil
        end
        subject.active_txn.should == nil
      end

      it 'should create read-only transactions' do
        subject.active_txn.should == nil
        subject.transaction(true) do |txn|
          subject.active_txn.should == txn
          txn.should be_instance_of(described_class)
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

      it 'should get environment' do
        env2 = nil
        env.transaction do |txn|
          env2 = txn.env
        end
        env2.should == env
      end
    end
  end

  describe LMDB::Database do
    subject { db }

    it 'should return flags' do
      subject.flags.should be_instance_of(Hash)
      subject.dupsort?.should == false
      subject.dupfixed?.should == false
    end

    it 'should support named databases' do
      main = env.database
      db1 = env.database('db1', :create => true)
      db2 = env.database('db2', :create => true)

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

      # check for key-value pairs on non-dupsort database
      subject.has?('cat', 'garfield').should == true
      subject.has?('cat', 'heathcliff').should == false
    end

    it 'should delete by key' do
      proc { subject.delete('cat') }.should raise_error(LMDB::Error::NOTFOUND)
      proc { subject.delete('cat', 'garfield') }.should raise_error(LMDB::Error::NOTFOUND)

      subject.put('cat', 'garfield')
      subject.delete('cat').should be_nil
      proc { subject.delete('cat') }.should raise_error(LMDB::Error::NOTFOUND)

      subject.put('cat', 'garfield')
      subject.delete('cat', 'garfield').should be_nil
      proc { subject.delete('cat', 'garfield') }.should raise_error(LMDB::Error::NOTFOUND)
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

    it 'should get environment' do
      main = env.database
      db1 = env.database('db1', :create => true)
      main.env.should == env
      db1.env.should == env
    end

    it 'should iterate over/list keys' do
      db['k1'] = 'v1'
      db['k2'] = 'v2'
      db.keys.sort.should == %w[k1 k2]
    end
  end

  describe LMDB::Cursor do
    before do
      db.put('key1', 'value1')
      db.put('key2', 'value2')
    end

    it 'should get first key/value' do
      db.cursor do |c|
        c.first.should == ['key1', 'value1']
      end
    end

    it 'should get last key/value' do
      db.cursor do |c|
        c.last.should == ['key2', 'value2']
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

    it 'should seek within range' do
      db.cursor do |c|
        db.put('key0', 'value0')
        c.first
        c.next_range('key1').should == ['key1', 'value1']
        c.next_range('key1').should == nil
      end
    end

    it 'should set to a key-value pair when db is dupsort' do
      dupdb = env.database 'dupsort', create: true, dupsort: true

      # check flag while we're at it
      dupdb.flags[:dupsort].should == true
      dupdb.dupsort?.should == true
      dupdb.dupfixed?.should == false

      dupdb.put 'key1', 'value1'
      dupdb.put 'key1', 'value2'
      dupdb.put 'key2', 'value3'
      dupdb.cursor do |c|
        c.set('key1', 'value2').should == ['key1', 'value2']
        c.set('key1', 'value1').should == ['key1', 'value1']
        c.set('key1', 'value3').should == nil
      end

      # this is basically an extended test of `cursor.set key, val`
      dupdb.has?('key1', 'value1').should == true
      dupdb.has?('key1', 'value2').should == true
      dupdb.has?('key1', 'value0').should == false

      # match the contents of key1
      dupdb.each_value('key1').to_a.sort.should == ['value1', 'value2']

      # we should have two entries for key1
      dupdb.cardinality('key1').should == 2

      dupdb.each_key.to_a.sort.should == ['key1', 'key2']
    end

    it 'should complain setting a key-value pair without dupsort' do
      db.cursor do |c|
        proc { c.set('key1', 'value1') }.should raise_error(LMDB::Error)
      end
    end

    it 'should raise without block or txn' do
      proc { db.cursor.next }.should raise_error(LMDB::Error)
    end

    it 'should raise outside txn' do
      c = nil
      env.transaction { c = db.cursor }
      proc { c.next }.should raise_error(LMDB::Error)
    end

    it 'should get database' do
      db2 = nil
      env.transaction { c = db.cursor; db2 = c.database }
      db2.should == db
    end
  end
end
