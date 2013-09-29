#!/usr/bin/env rake

require 'bundler/setup'
require 'rspec/core/rake_task'
require 'rake/extensiontask'

RSpec::Core::RakeTask.new :spec
Rake::ExtensionTask.new :lmdb_ext

task :default => [:compile, :spec]
