#!/usr/bin/env rake

require 'rake/testtask'

desc "Build the extension"
task :build do
  ext = File.expand_path("../ext", __FILE__)
  sh "cd #{ext} && ruby extconf.rb && make"
end

desc "Delete the build"
task :clean do
  ext = File.expand_path("../ext", __FILE__)
  sh "cd #{ext} && ruby extconf.rb && make clean"
end

Rake::TestTask.new :test do |t|
  t.libs.push "ext"
  t.test_files = FileList['spec/*_spec.rb']
  t.verbose = true
end

task :test => :build

desc 'Default: run test.'
task :default => :test
