#
#  pgruby.gemspec  --  PgRuby Gem specification
#

require "rubygems"

SPEC = Gem::Specification.new do |s|
  s.name              = "pgsgl"
  s.rubyforge_project = "pgsgl"
  s.version           = "0.9"
  s.summary           = "PostgreSQL-API for Ruby"
  s.authors           = [ "Bertram Scharpf"]
  s.email             = "<software@bertram-scharpf.de>"
  s.homepage          = "http://www.bertram-scharpf.de"
  s.requirements      = "PostgreSQL"
  s.has_rdoc          = true
  s.require_path      = "."
  s.extensions        = "mkrf_conf"
  s.files             = Dir[ "**/*"].reject { |fn| fn =~ /\.gem$/ }
end

if $0 == __FILE__ then
  Gem::Builder.new( SPEC).build
end

