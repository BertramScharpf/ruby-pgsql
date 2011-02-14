#
#  pgsql.gemspec  --  PgSql Gem specification
#

require "rubygems"

class Gem::Specification
  RE_VERSION = /^\s*rb_define_const\(\s*\w+,\s*"VERSION",\s*.*"([^"]+)".*\);$/
  def extract_version
    File.open "lib/module.c" do |f|
      f.each_line { |l|
        l =~ RE_VERSION and return $1
      }
    end
    nil
  end
end

SPEC = Gem::Specification.new do |s|
  s.name              = "pgsql"
  s.rubyforge_project = "pgsql"
  s.version           = s.extract_version
  s.summary           = "PostgreSQL-API for Ruby"
  s.description       = <<EOT
This is not the official PostgreSQL library that was originally written by Guy
Decoux. As the project wasn't maintained a long time after Guy's desease, I
decided to fork my own project.
EOT
  s.authors           = [ "Bertram Scharpf"]
  s.email             = "<software@bertram-scharpf.de>"
  s.homepage          = "http://www.bertram-scharpf.de"
  s.requirements      = "PostgreSQL"
  s.has_rdoc          = true
  s.extensions        = "lib/mkrf_conf"
  s.executables       = %w(
                          pgbackup
                        )
  s.files             = %w(
                          lib/Rakefile
                          lib/mkrf_conf
                          lib/undef.h
                          lib/row.h
                          lib/row.c
                          lib/result.h
                          lib/result.c
                          lib/large.h
                          lib/large.c
                          lib/conn.h
                          lib/conn.c
                          lib/module.h
                          lib/module.c
                        )
  s.extra_rdoc_files = %w(
                          README
                          LICENSE
                        )
  s.rdoc_options.push   %w(--charset utf-8 --main README)
end

if $0 == __FILE__ then
  Gem::Builder.new( SPEC).build
end

