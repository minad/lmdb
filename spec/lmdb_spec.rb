require 'spec_helper'

describe LMDB do

  describe "constants" do
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
  end

end