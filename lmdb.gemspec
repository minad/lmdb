# -*- encoding: utf-8 -*-
require File.dirname(__FILE__) + '/lib/lmdb/version'
require 'date'

Gem::Specification.new do |s|
  s.name        = File.basename(__FILE__, '.gemspec')
  s.version     = LMDB::VERSION
  s.platform    = Gem::Platform::RUBY
  s.date        = Date.today.to_s
  s.licenses    = ['MIT']
  s.summary     = 'Ruby bindings to Lightning MDB'
  s.email       = 'mail@daniel-mendler.de'
  s.homepage    = 'https://github.com/minad/lmdb'
  s.description = 'lmdb is a Ruby binding to OpenLDAP Lightning MDB.'
  s.authors     = ['Daniel Mendler']
  s.extensions  = Dir['ext/**/extconf.rb']

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- spec/*`.split("\n")
  s.require_paths = ['lib']

  s.required_ruby_version = ">= 2.4"
  s.add_development_dependency 'rake', "~> 13.0"
  s.add_development_dependency 'rake-compiler', '~> 1.1'
  s.add_development_dependency 'rspec', "~> 3.0"
end
