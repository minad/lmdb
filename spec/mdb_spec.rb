require 'spec_helper'

describe MDB do

  describe "constants" do
    it 'has version constants' do
      MDB::VERSION_MAJOR.should be_instance_of(Fixnum)
      MDB::VERSION_MINOR.should be_instance_of(Fixnum)
      MDB::VERSION_PATCH.should be_instance_of(Fixnum)
      MDB::VERSION.should be_instance_of(String)
    end

    it 'has environment flags' do
      MDB::FIXEDMAP.should be_instance_of(Fixnum)
      MDB::NOSUBDIR.should be_instance_of(Fixnum)
      MDB::NOSYNC.should be_instance_of(Fixnum)
      MDB::RDONLY.should be_instance_of(Fixnum)
      MDB::NOMETASYNC.should be_instance_of(Fixnum)
      MDB::WRITEMAP.should be_instance_of(Fixnum)
      MDB::MAPASYNC.should be_instance_of(Fixnum)
    end

    it 'has database flags' do
      MDB::REVERSEKEY.should be_instance_of(Fixnum)
      MDB::DUPSORT.should be_instance_of(Fixnum)
      MDB::INTEGERKEY.should be_instance_of(Fixnum)
      MDB::DUPFIXED.should be_instance_of(Fixnum)
      MDB::INTEGERDUP.should be_instance_of(Fixnum)
      MDB::REVERSEDUP.should be_instance_of(Fixnum)
      MDB::CREATE.should be_instance_of(Fixnum)
      MDB::NOOVERWRITE.should be_instance_of(Fixnum)
      MDB::NODUPDATA.should be_instance_of(Fixnum)
      MDB::CURRENT.should be_instance_of(Fixnum)
      MDB::RESERVE.should be_instance_of(Fixnum)
      MDB::APPEND.should be_instance_of(Fixnum)
      MDB::APPENDDUP.should be_instance_of(Fixnum)
      MDB::MULTIPLE.should be_instance_of(Fixnum)
    end
  end

end