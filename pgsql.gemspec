#
#  pgsql.gemspec  --  PgSql Gem specification
#

class Gem::Specification
  def extract_definition name
    re = /^\s*#\s*define\s+#{name}\s+"([0-9a-zA-Z_.-]+)"/
    File.open "lib/module.c" do |f|
      f.each_line { |l|
        l =~ re and return $1
      }
    end
    nil
  end
end

Gem::Specification.new do |s|
  s.name              = "pgsql"
  s.version           = s.extract_definition "PGSQL_VERSION"
  s.summary           = "PostgreSQL-API for Ruby"
  s.description       = <<EOT
This is not the official PostgreSQL library that was originally written by Guy
Decoux. As the project wasn't maintained a long time after Guy's decease, I
decided to fork my own project.
EOT
  s.authors           = [ "Bertram Scharpf"]
  s.email             = "<software@bertram-scharpf.de>"
  s.homepage          = "http://www.bertram-scharpf.de/software/pgsql"

  s.requirements      = "PostgreSQL"
  s.add_dependency      "autorake", ">=2.0"

  s.extensions        = "lib/mkrf_conf"
  s.executables       = %w(
                        )
  s.files             = %w(
                          lib/Rakefile
                          lib/mkrf_conf
                          lib/undef.h
                          lib/module.h
                          lib/module.c
                          lib/conn.h
                          lib/conn.c
                          lib/conn_quote.h
                          lib/conn_quote.c
                          lib/conn_exec.h
                          lib/conn_exec.c
                          lib/result.h
                          lib/result.c
                        )
  s.extra_rdoc_files = %w(
                          README
                          LICENSE
                        )

  s.has_rdoc          = true
  s.rdoc_options.concat %w(--charset utf-8 --main README)
end

