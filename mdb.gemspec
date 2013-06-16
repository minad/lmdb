# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name        = "mdb"
  s.version     = "0.1.0"
  s.platform    = Gem::Platform::RUBY
  s.licenses    = ["MIT"]
  s.summary     = "Ruby bindings to LMDB"
  s.email       = "mail@daniel-mendler.de"
  s.homepage    = "https://github.com/minad/mdb"
  s.description = "mdb is a Ruby binding to LMDB."
  s.authors     = ["Daniel Mendler", "Black Square Media"]
  s.extensions  = Dir["ext/**/extconf.rb"]

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- spec/*`.split("\n")
  s.require_paths = ["lib"]

  s.add_development_dependency "rake"
  s.add_development_dependency "rake-compiler"
  s.add_development_dependency "rspec"
end
