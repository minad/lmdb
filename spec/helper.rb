require 'lmdb'
require 'rspec'
require 'fileutils'

SPEC_ROOT = File.dirname(__FILE__)
TEMP_ROOT = File.join(SPEC_ROOT, 'tmp')

module LMDB::SpecHelper
  def mkpath(name = 'env')
    path = File.join(TEMP_ROOT, name)
    FileUtils.mkpath(path)
    path
  end

  def path
    @path ||= mkpath
  end

  def env
    @env ||= LMDB::Environment.new :path => path
  end
end

RSpec.configure do |c|
  c.include LMDB::SpecHelper
  c.after { FileUtils.rm_rf TEMP_ROOT }
  c.expect_with :rspec do |cc|
    cc.syntax = :should
  end
end
