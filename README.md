# pgsql Ruby Gem

A PostgreSQL library that was carefully designed.


## Author

Bertram Scharpf <software@bertram-scharpf.de>


## Features

  * Connection parameters from hash
  * Query parameters
  * Asynchronous queries
  * Quick query of single lines or values
  * Full PostgreSQL quoting support
  * Built-in transactions and savepoints by Ruby blocks


## Example

Write something like this:

```ruby
require "pgsql"

Pg::Conn.open :dbname => "test1", :user => "jdoe" do |conn|
  conn.exec "SELECT * FROM mytable;" do |result|
  result.each { |row|
    l = row.join ", "
    ...
  }
  end
  cmd = <<~ENDSQL
    SELECT * FROM mytable WHERE num=$1::INTEGER;
  ENDSQL
  conn.query cmd, 42 do |row|
    l = row.join ", "
    ...
  end
  ...
end
```


## Thanks

In the remembrance of Guy Decoux.


## Copyright

  * (C) 2011-2025 Bertram Scharpf <software@bertram-scharpf.de>
  * License: [BSD-2-Clause+](./LICENSE)
  * Repository: [ruby-pgsql](https://github.com/BertramScharpf/ruby-pgsql)

