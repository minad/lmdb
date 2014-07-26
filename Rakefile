#!/usr/bin/env rake

$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), "lib"))
GEMSPEC = Dir['*.gemspec'].first
PRJ = File.basename(GEMSPEC, ".gemspec")

require 'bundler/setup'
require 'rspec/core/rake_task'
require 'rake/extensiontask'

RSpec::Core::RakeTask.new :spec
Rake::ExtensionTask.new :lmdb_ext

task :default => [:compile, :spec]

def version
  @version ||= begin
    require "#{PRJ}/version"
    warn "LMDB::VERSION not a string" unless LMDB::VERSION.kind_of? String
    LMDB::VERSION
  end
end

def tag
  @tag ||= "v#{version}"
end

def latest
  @latest ||= `git describe --abbrev=0 --tags --match 'v*'`.chomp
end

desc "Commit, tag, and push repo; build and push gem"
task :release => "release:is_new_version" do
  require 'tempfile'

  sh "gem build #{GEMSPEC}"

  file = Tempfile.new "template"
  begin
    file.puts "release #{version}"
    file.close
    sh "git commit --allow-empty -a -v -t #{file.path}"
  ensure
    file.close unless file.closed?
    file.unlink
  end

  sh "git tag #{tag}"
  sh "git push"
  sh "git push --tags"

  sh "gem push #{PRJ}-#{version}.gem"
end

namespace :release do
  desc "Diff to latest release"
  task :diff do
    sh "git diff #{latest}"
  end

  desc "Log to latest release"
  task :log do
    sh "git log #{latest}.."
  end

  task :is_new_version do
    abort "#{tag} exists; update version!" unless `git tag -l #{tag}`.empty?
  end
end
