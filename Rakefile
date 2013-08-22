#!/usr/bin/env rake

require 'bundler/setup'
require 'rspec/core/rake_task'
require "rake/extensiontask"

# Install spec tasks
RSpec::Core::RakeTask.new :spec

# Install compile task
Rake::ExtensionTask.new('lmdb_ext')

desc 'Default: compile & run specs.'
task default: [:compile, :spec]
