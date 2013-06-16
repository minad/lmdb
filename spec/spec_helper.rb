require 'bundler/setup'
require 'mdb'
require 'rspec'
require 'fileutils'
require 'pathname'

SPEC_ROOT = Pathname.new File.expand_path('..', __FILE__)
TEMP_ROOT = SPEC_ROOT.join("tmp")

module MDB::SpecHelper

  def mkpath(name = 'env')
    TEMP_ROOT.join(name).to_s.tap do |path|
      FileUtils.mkpath(path)
    end
  end

  def path
    @path ||= mkpath
  end

  def env
    @env ||= MDB::Environment.new path: path
  end

end

RSpec.configure do |c|
  c.filter_run_excluding segfault: true
  c.include MDB::SpecHelper
  c.after { FileUtils.rm_rf TEMP_ROOT.to_s }
end

