require 'spec_helper'

describe MDB::Environment do

  subject    { env }

  it  { should be_instance_of(described_class) }
  its(:path) { should be_instance_of(String) }
  its(:info) { should be_instance_of(Hash) }
  its("info.keys") { should =~ [:address, :branch_pages, :depth, :entries, :last_page, :last_transaction_id, :leaf_pages, :max_size, :maxreaders, :overflow_pages, :page_size, :readers] }

  it 'should sync' do
    subject.sync.should be(true)
    subject.sync(force: true).should be(true)
  end

  it 'should open databases' do
    subject.database(:pets).should be_instance_of(MDB::Database)
    subject.db(:pets).should be_instance_of(MDB::Database)
  end

end