# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name        = "mdb-ruby"
  s.version     = "0.0.1"
  s.platform    = Gem::Platform::RUBY
  s.licenses    = ["MIT"]
  s.summary     = "a Ruby binding to LMDB"
  s.email       = "mail@daniel-mendler.de"
  s.homepage    = "https://github.com/minad/mdb"
  s.description = "mdb-ruby is a Ruby binding to LMDB."
  s.authors     = ["Daniel Mendler"]
  s.extensions  = %w(ext/extconf.rb)

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- spec/*`.split("\n")
  s.require_paths = ["ext"]

  s.add_development_dependency("rake")
end
