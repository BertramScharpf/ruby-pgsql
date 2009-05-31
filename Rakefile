#
#  Rakefile  --  build some libraries
#


require "rake/rakonfig"

extend Rake::Configure


rule ".o" => ".c" do |t|
  cc t.name, t.source, "-O2", "-fPIC"
end


DLs = {
  "pgsql.so"  => "pgsql.o",
}

DLs.each { |k,v|
  task k => v do |t|
    ld t.name, t.prerequisites, "-shared"
  end
}


task :default => DLs.keys


task :clean do
  FileList[ "*.o", "*.so"].each { |f| rm_f f }
end

