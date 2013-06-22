require 'spec_helper'

describe LMDB::Database do

  subject    { env.db(:pets) }
  after      { subject.close }

  its(:name) { should == "pets" }
  its(:env)  { should == env }

  it 'should get/set data' do
    subject.get("cat").should be_nil
    subject.set("cat", "garfield").should be_nil
    subject.get("cat").should == "garfield"
  end

  it 'should get/set data' do
    subject.get("cat").should be_nil
    subject.set("cat", "garfield").should be_nil
    subject.get("cat").should == "garfield"
  end

end