#!/usr/bin/env rake

require 'bundler/setup'
require 'rspec/core/rake_task'
require "rake/extensiontask"

gemspec = Bundler.load_gemspec(File.expand_path("../mdb-rb.gemspec", __FILE__))

RSpec::Core::RakeTask.new :spec

Rake::ExtensionTask.new :mdb_ext, gemspec do |ext|
  ext.ext_dir = 'ext/mdb'
  ext.lib_dir = 'lib/mdb'
end

desc 'Default: run specs.'
task :default => :spec
