#!/usr/bin/env rake

require 'bundler/setup'
require 'rspec/core/rake_task'
require "rake/extensiontask"

# Install spec tasks
RSpec::Core::RakeTask.new :spec

# Install compile task
Rake::ExtensionTask.new :mdb_ext do |ext|
  ext.ext_dir = 'ext/mdb'
  ext.lib_dir = 'lib/mdb'
end

desc 'Default: compile & run specs.'
task default: [:compile, :spec]
