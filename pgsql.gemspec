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
  s.authors           = [ "Bertram Scharpf"]
  s.email             = "<software@bertram-scharpf.de>"
  s.homepage          = "https://github.com/BertramScharpf/ruby-pgsql.git"
  s.license           = "BSD-2-Clause"

  s.summary           = "PostgreSQL-API for Ruby"
  s.description       = <<~EOT
    This is not the official PostgreSQL library that was originally written by
    Guy Decoux. As the project wasn't maintained a long time after Guy's
    decease, I decided to create my own project.
  EOT

  s.required_ruby_version = ">= 3.1.0"
  s.requirements          = "PostgreSQL"
  s.add_dependency          "autorake", "~>2.0"
  s.add_dependency          "rake",     "~>13.0"

  s.extensions        = "lib/mkrf_conf"
  s.files             = %w(lib/Rakefile lib/mkrf_conf) + Dir[ "lib/**/*.[ch]"]

  s.executables       = %w(
                        )

  s.extra_rdoc_files = %w(
                          README.md
                          LICENSE
                        )

  s.rdoc_options.concat %w(--main README)
end

