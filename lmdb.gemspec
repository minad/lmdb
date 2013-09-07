# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name        = File.basename(__FILE__, '.gemspec')
  s.version     = '0.2.0'
  s.platform    = Gem::Platform::RUBY
  s.licenses    = ['MIT']
  s.summary     = 'Ruby bindings to Lightning MDB'
  s.email       = 'mail@daniel-mendler.de'
  s.homepage    = 'https://github.com/minad/lmdb'
  s.description = 'imdb is a Ruby binding to OpenLDAP Lightning MDB.'
  s.authors     = ['Daniel Mendler']
  s.extensions  = Dir['ext/**/extconf.rb']

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- spec/*`.split("\n")
  s.require_paths = ['lib']

  s.add_development_dependency 'rake'
  s.add_development_dependency 'rake-compiler'
  s.add_development_dependency 'rspec'
end
