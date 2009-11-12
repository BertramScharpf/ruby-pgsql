#
#  pgsql.gemspec  --  PgSql Gem specification
#

require "rubygems"

class Gem::Specification
  def extract_version
    File.open "pgsql.c" do |f|
      f.each_line { |l|
        l =~ /^\s*#define\s+VERSION\s+"(.*)"\s*$/ and return $1
      }
    end
  end
end

SPEC = Gem::Specification.new do |s|
  s.name              = "pgsql"
  s.rubyforge_project = "pgsql"
  s.version           = s.extract_version
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

