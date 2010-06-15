#
#  pgsql.gemspec  --  PgSql Gem specification
#

require "rubygems"

class Gem::Specification
  def extract_version
    File.open "lib/module.h" do |f|
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
  s.description       = <<EOT
This was originally written by Guy Decoux. As the project wasn't
maintained a long time after his death, I decided to fork my own
version.
EOT
  s.authors           = [ "Bertram Scharpf"]
  s.email             = "<software@bertram-scharpf.de>"
  s.homepage          = "http://www.bertram-scharpf.de"
  s.requirements      = "PostgreSQL"
  s.has_rdoc          = true
  s.extensions        = "lib/mkrf_conf"
  s.files             = %w(
                          lib/Rakefile
                          lib/mkrf_conf
                          lib/pgsql.c
                          lib/pgsql.h
                          lib/module.c
                          lib/module.h
                          lib/undef.h
                          bin/pgbackup
                        )
end

if $0 == __FILE__ then
  Gem::Builder.new( SPEC).build
end

