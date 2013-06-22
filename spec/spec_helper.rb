require 'bundler/setup'
require 'lmdb'
require 'rspec'
require 'fileutils'
require 'pathname'

SPEC_ROOT = Pathname.new File.expand_path('..', __FILE__)
TEMP_ROOT = SPEC_ROOT.join("tmp")

module LMDB::SpecHelper

  def mkpath(name = 'env')
    TEMP_ROOT.join(name).to_s.tap do |path|
      FileUtils.mkpath(path)
    end
  end

  def path
    @path ||= mkpath
  end

  def env
    @env ||= LMDB::Environment.new path: path
  end

end

RSpec.configure do |c|
  c.filter_run_excluding segfault: true
  c.include LMDB::SpecHelper
  c.after { FileUtils.rm_rf TEMP_ROOT.to_s }
end

