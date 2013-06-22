require 'spec_helper'

shared_examples "an environment" do
  describe 'open' do
    it 'returns environment' do
      env = subject.open(path)
      env.should be_instance_of(described_class::Environment)
      env.close
    end

    it 'accepts block' do
      subject.open(path) do |env|
        env.should be_instance_of(described_class::Environment)
        42
      end.should == 42
    end

    it 'accepts options' do
      env = subject.open(path, :flags => LMDB::NOSYNC, :mode => 0777, :maxreaders => 777, :mapsize => 111111, :maxdbs => 666)
      env.should be_instance_of(described_class::Environment)
      env.info.maxreaders.should == 777
      env.info.mapsize.should == 111111
      env.close
    end
  end
end

describe LMDB::Ext do

  let(:env)  { LMDB::Ext.open(path) }
  after      { env.close }

  it_behaves_like "an environment" do
    subject { LMDB::Ext }
  end

  describe "Stat" do
    subject { env.stat }

    it { should be_instance_of(described_class::Stat) }
    its(:psize) { should be_instance_of(Fixnum) }
    its(:depth) { should be_instance_of(Fixnum) }
    its(:branch_pages) { should be_instance_of(Fixnum) }
    its(:leaf_pages) { should be_instance_of(Fixnum) }
    its(:overflow_pages) { should be_instance_of(Fixnum) }
    its(:entries) { should be_instance_of(Fixnum) }
  end

  describe "Info" do
    subject { env.info }

    it { should be_instance_of(described_class::Info) }
    its(:mapaddr) { should be_instance_of(Fixnum) }
    its(:mapsize) { should be_instance_of(Fixnum) }
    its(:last_pgno) { should be_instance_of(Fixnum) }
    its(:last_txnid) { should be_instance_of(Fixnum) }
    its(:maxreaders) { should be_instance_of(Fixnum) }
    its(:numreaders) { should be_instance_of(Fixnum) }
  end

  describe "Environment" do
    it_behaves_like "an environment" do
      subject { described_class::Environment }
    end

    subject { env }

    its(:path)  { should == path }
    its(:flags) { should == 0 }

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
      txn = subject.transaction
      txn.should be_instance_of(described_class::Transaction)
      txn.parent.should be_nil
      txn.environment.should == subject
      txn.abort
    end

    it 'should create read-only transactions' do
      txn = subject.transaction(true)
      txn.should be_instance_of(described_class::Transaction)
      txn.abort
    end

    it 'should create transactions blocks' do
      subject.transaction do |txn|
        txn.should be_instance_of(described_class::Transaction)
        txn.environment.should == subject
        txn.parent.should be_nil
      end
    end
  end

  describe "Transaction" do
    subject { env.transaction }
    after   { subject.abort rescue nil }

    its(:environment) { should == env }
    its(:parent) { should be_nil }

    it 'can create child transactions' do
      txn = subject.transaction
      txn.should be_instance_of(described_class::Transaction)
      txn.parent.should == subject
      txn.environment.should == env
      txn.abort
    end

    it 'can create block transactions' do
      subject.transaction do |txn|
        txn.should be_instance_of(described_class::Transaction)
        txn.parent.should == subject
      end
    end
  end

  describe "Database" do

    subject do
      env.transaction do |txn|
        env.open(txn, 'db', LMDB::CREATE)
      end
    end
    let!(:db) { subject }
    after     { subject.close rescue nil }

    it 'stores key/values in same transaction' do
      env.transaction do |txn|
        db.put(txn, 'key', 'value').should be_nil
        db.get(txn, 'key').should == 'value'
      end
    end

    it 'stores key/values in different transactions' do
      env.transaction do |txn|
        db.put(txn, 'key', 'value').should be_nil
      end

      env.transaction do |txn|
        db.get(txn, 'key').should == 'value'
      end
    end

    it 'should return cursor' do
      env.transaction do |txn|
        cursor = db.cursor(txn)
        cursor.should be_instance_of(described_class::Cursor)
        cursor.close
      end
    end

    it 'should return stat' do
      env.transaction do |txn|
        db.stat(txn).should be_instance_of(described_class::Stat)
      end
    end

    it 'should close' do
      db.close.should be_nil
      -> { db.close }.should raise_error(LMDB::Ext::Error, /closed/)
    end

    it "should be correctly GC'd", segfault: true do
      db = env.transaction {|t| env.open(t, 'db', LMDB::CREATE) }
      db = nil
      GC.start
      db = env.transaction {|t| env.open(t, 'db', LMDB::CREATE) }
    end

  end

  describe "Cursor" do

    let! :db do
      env.transaction {|txn| env.open(txn, 'db', LMDB::CREATE) }
    end

    before do
      env.transaction do |txn|
        db.put(txn, 'key1', 'value1')
        db.put(txn, 'key2', 'value2')
      end
    end

    after do
      db.close
    end

    def with_cursor
      env.transaction do |txn|
        cursor = db.cursor(txn)
        begin
          yield cursor
        ensure
          cursor.close
        end
      end
    end

    it 'should get next key/value' do
      with_cursor do |c|
        c.first.should == ['key1', 'value1']
      end
    end

    it 'should get next key/value' do
      with_cursor do |c|
        c.first
        c.next.should == ['key2', 'value2']
      end
    end

    it 'should seek to key' do
      with_cursor do |c|
        c.set('key1').should == ['key1', 'value1']
      end
    end

    it 'should seek to closest key' do
      with_cursor do |c|
        c.set_range('key0').should == ['key1', 'value1']
      end
    end

    it 'should seek to key with nuls' do
      with_cursor do |c|
        c.set_range("\x00").should == ['key1', 'value1']
      end
    end

  end
end
