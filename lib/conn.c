/*
 *  conn.c  --  Pg connection
 */


#include "conn.h"

#include <st.h>


static PGconn *try_connectdb( VALUE arg);
static PGconn *try_setdbLogin( VALUE args);
static int     build_key_value_string_i( VALUE key, VALUE value, VALUE result);


static VALUE pgconn_s_connect( int argc, VALUE *argv, VALUE klass);
static VALUE pgconn_s_escape( VALUE self, VALUE string);
static VALUE pgconn_s_escape_bytea( VALUE self, VALUE obj);
static VALUE pgconn_s_unescape_bytea( VALUE self, VALUE obj);
static VALUE pgconn_s_translate_results_set( VALUE self, VALUE fact);

static VALUE pgconn_alloc( VALUE klass);
static VALUE pgconn_init( int argc, VALUE *argv, VALUE self);
static VALUE pgconn_close( VALUE obj);
static VALUE pgconn_reset( VALUE obj);
static VALUE pgconn_protocol_version( VALUE obj);
static VALUE pgconn_server_version( VALUE obj);
static VALUE pgconn_db( VALUE obj);
static VALUE pgconn_host( VALUE obj);
static VALUE pgconn_options( VALUE obj);
static VALUE pgconn_port( VALUE obj);
static VALUE pgconn_tty( VALUE obj);
static VALUE pgconn_user( VALUE obj);
static VALUE pgconn_status( VALUE obj);
static VALUE pgconn_error( VALUE obj);


static VALUE pgconn_loimport( VALUE obj, VALUE filename);
static VALUE pgconn_loexport( VALUE obj, VALUE lo_oid, VALUE filename);
static VALUE pgconn_lounlink( VALUE obj, VALUE lo_oid);
static VALUE pgconn_locreate( int argc, VALUE *argv, VALUE obj);
static VALUE pgconn_loopen( int argc, VALUE *argv, VALUE obj);



static VALUE rb_cRational;
static VALUE rb_cDate;
static VALUE rb_cDateTime;

VALUE rb_cPGConn;

int translate_results = 1;


#define SINGLE_QUOTE '\''


PGconn*
get_pgconn( obj)
    VALUE obj;
{
    PGconn *conn;

    Data_Get_Struct( obj, PGconn, conn);
    if (conn == NULL)
        rb_raise( rb_ePGError, "closed connection");
    return conn;
}

PGconn *
try_connectdb( arg)
    VALUE arg;
{
    VALUE conninfo;

    if      (!NIL_P( conninfo = rb_check_string_type( arg)))
        ;
    else if (!NIL_P( conninfo = rb_check_hash_type( arg))) {
        VALUE key_values = rb_ary_new2( RHASH( conninfo)->tbl->num_entries);
        rb_hash_foreach( conninfo, &build_key_value_string_i, key_values);
        conninfo = rb_ary_join( key_values, rb_str_new2( " "));
    } else
        return NULL;
    return PQconnectdb( STR2CSTR( conninfo));
}

PGconn *
try_setdbLogin( args)
    VALUE args;
{
    VALUE temp;
    char *host, *port, *opt, *tty, *dbname, *login, *pwd;
    host=port=opt=tty=dbname=login=pwd=NULL;

    rb_funcall( args, rb_intern( "flatten!"), 0);

    AssignCheckedStringValue( host, rb_ary_entry( args, 0));
    if (!NIL_P( temp = rb_ary_entry( args, 1)) && NUM2INT( temp) != -1) {
        temp = rb_obj_as_string( temp);
        port = STR2CSTR( temp);
    }
    AssignCheckedStringValue( opt,    rb_ary_entry( args, 2));
    AssignCheckedStringValue( tty,    rb_ary_entry( args, 3));
    AssignCheckedStringValue( dbname, rb_ary_entry( args, 4));
    AssignCheckedStringValue( login,  rb_ary_entry( args, 5));
    AssignCheckedStringValue( pwd,    rb_ary_entry( args, 6));

    return PQsetdbLogin( host, port, opt, tty, dbname, login, pwd);
}

int
build_key_value_string_i( key, value, result)
    VALUE key, value, result;
{
    VALUE key_value;
    if (key == Qundef) return ST_CONTINUE;
#if 0
    key_value = rb_obj_class( key) == rb_cString ?
        rb_str_dup( key) : rb_funcall( key, rb_intern( "to_s"), 0);
#else
    key_value = rb_obj_as_string( key);
#endif
    rb_str_cat( key_value, "=", 1);
    rb_str_concat( key_value, value);
    rb_ary_push( result, key_value);
    return ST_CONTINUE;
}



/*
 * Document-method: connect
 *
 * call-seq:
 *     Pg::Conn.connect( connection_hash, ...) -> Pg::Conn
 *     Pg::Conn.connect( connection_string)    -> Pg::Conn
 *     Pg::Conn.connect( host, port, options, tty, dbname, login, passwd)
 *                                             -> Pg::Conn
 *     Pg::Conn.connect( ...) { |conn| ... }
 *
 *  _host_::     server hostname
 *  _port_::     server port number
 *  _options_::  backend options (String)
 *  _tty_::      tty to print backend debug message <i>(ignored in newer
 *                  versions of PostgreSQL)</i> (String)
 *  _dbname_::   connecting database name
 *  _login_::    login user name
 *  _passwd_::   login password
 *
 *  If a block is given, the connection is closed after the block was
 *  executed.
 *
 *  On failure, it raises a Pg::Error exception.
 */
VALUE
pgconn_s_connect( argc, argv, klass)
    int argc;
    VALUE *argv;
    VALUE klass;
{
    VALUE pgconn;

    pgconn = rb_class_new_instance( argc, argv, klass);
    return rb_block_given_p() ?
        rb_ensure( rb_yield, pgconn, pgconn_close, pgconn) : pgconn;
}

/*
 * call-seq:
 *    Pg::Conn.escape( str)   -> str
 *
 * Returns a SQL-safe version of the String _str_. Unlike #quote, does not
 * wrap the String in '...'.
 */
VALUE
pgconn_s_escape( self, string)
    VALUE self;
    VALUE string;
{
    char* escaped;
    int size;
    VALUE result;

    Check_Type( string, T_STRING);

    escaped = ALLOCA_N( char, RSTRING( string)->len * 2 + 1);
    size = PQescapeString( escaped, RSTRING( string)->ptr, RSTRING( string)->len);
    result = rb_str_new( escaped, size);
    OBJ_INFECT( result, string);
    return result;
}

/*
 * call-seq:
 *    Pg::Conn.quote( obj)
 *    Pg::Conn.quote( obj) { |obj| ... }
 *    Pg::Conn.format( obj)
 *    Pg::Conn.format( obj) { |obj| ... }
 *
 * If _obj_ has a method +to_postgres+, let that determine the String
 * representation for use in PostgreSQL.
 *
 * If _obj_ is a Number, String, Array, Date, Time, DateTime, +nil+,
 * +true+, or +false+ then #quote returns a String representation of
 * that object safe for use in PostgreSQL.
 *
 * If _obj_ is not one of the above classes and a block is supplied to #quote,
 * the block is invoked, passing along the object. The return value from the
 * block is returned as a string.
 *
 * If _obj_ is not one of the recognized classes and no block is supplied,
 * a Pg::Error is raised.
 */
static VALUE pgconn_s_quote( VALUE self, VALUE obj);
VALUE
pgconn_s_quote( self, obj)
    VALUE self, obj;
{
    char* quoted;
    int size;
    VALUE result;

    if (TYPE( obj) == T_STRING) {
        quoted = ALLOCA_N( char, RSTRING( obj)->len * 2 + 2 + 1 + 1);
        quoted[ 0] = 'E';
        quoted[ 1] = SINGLE_QUOTE;
        size = PQescapeString( quoted + 2, RSTRING( obj)->ptr, RSTRING( obj)->len);
        quoted[ size + 2] = SINGLE_QUOTE;
        result = rb_str_new( quoted, size + 2 + 1);
        OBJ_INFECT( result, obj);
        return result;
    }
    else {
        ID pg_to_postgres;

        pg_to_postgres = rb_intern( "to_postgres");
        if (rb_respond_to( obj, pg_to_postgres)) {
            result = rb_funcall( obj, pg_to_postgres, 0);
            if (OBJ_TAINTED( obj)) OBJ_TAINT( result);
        }
        else {
            result = format_single_element( obj);
            if (result == Qundef) {
                if (CLASS_OF( obj) == rb_cRational) {
                    result = rb_obj_as_string( obj);
                    rb_str_buf_cat2( result, ".0");
                } else {
                    result = rb_str_buf_new2( "'");
                    if (CLASS_OF( obj) == rb_cTime) {
                        rb_str_buf_append( result,
                            rb_funcall( obj, rb_intern( "iso8601"), 0));
                        rb_str_buf_cat2( result, "'::timestamptz");
                    }
                    else if (CLASS_OF( obj) == rb_cDate) {
                        rb_str_buf_append( result, rb_obj_as_string( obj));
                        rb_str_buf_cat2( result, "'::date");
                    }
                    else if (CLASS_OF( obj) == rb_cDateTime) {
                        rb_str_buf_append( result, rb_obj_as_string( obj));
                        rb_str_buf_cat2( result, "'::timestamptz");
                    } else {
                        rb_str_buf_append( result, rb_obj_as_string( obj));
                        rb_str_buf_cat2( result, "'::unknown");
                    }
                }
                if (OBJ_TAINTED( obj)) OBJ_TAINT( result);
            }
        }
        return result;
    }
}

/*
 * call-seq:
 *   Pg::Conn.escape_bytea( obj)
 *
 * Escapes binary data for use within an SQL command with the type +bytea+.
 *
 * Certain byte values must be escaped (but all byte values may be escaped)
 * when used as part of a +bytea+ literal in an SQL statement. In general, to
 * escape a byte, it is converted into the three digit octal number equal to
 * the octet value, and preceded by two backslashes. The single quote (') and
 * backslash (\) characters have special alternative escape sequences.
 * #escape_bytea performs this operation, escaping only the minimally required
 * bytes.
 *
 * See the PostgreSQL documentation on PQescapeBytea
 * [http://www.postgresql.org/docs/current/interactive/libpq-exec.html#LIBPQ-PQESCAPEBYTEACONN]
 * for more information.
 */
VALUE
pgconn_s_escape_bytea( self, obj)
    VALUE self;
    VALUE obj;
{
    char *from, *to;
    size_t from_len, to_len;
    VALUE ret;

    Check_Type( obj, T_STRING);
    from      = RSTRING( obj)->ptr;
    from_len  = RSTRING( obj)->len;

    to = (char *) PQescapeByteaConn( get_pgconn( obj),
                    (unsigned char *) from, from_len, &to_len);

    ret = rb_str_new( to, to_len - 1);
    OBJ_INFECT( ret, obj);

    PQfreemem( to);

    return ret;
}

/*
 * call-seq:
 *   Pg::Conn.unescape_bytea( obj )
 *
 * Converts an escaped string representation of binary data into binary data
 * --- the reverse of #escape_bytea. This is needed when retrieving +bytea+
 *  data in text format, but not when retrieving it in binary format.
 *
 * See the PostgreSQL documentation on PQunescapeBytea
 * [http://www.postgresql.org/docs/current/interactive/libpq-exec.html#LIBPQ-EXEC-ESCAPE-BYTEA]
 * for more information.
 */
VALUE
pgconn_s_unescape_bytea( self, obj)
    VALUE self, obj;
{
    char *from, *to;
    size_t to_len;
    VALUE ret;

    Check_Type( obj, T_STRING);
    from = STR2CSTR( obj);

    to = (char *) PQunescapeBytea( (unsigned char *)from, &to_len);

    ret = rb_str_new( to, to_len);
    OBJ_INFECT( ret, obj);
    PQfreemem( to);

    return ret;
}

/*
 * call-seq:
 *   Pg::Conn.translate_results = boolean
 *
 * When true (default), results are translated to appropriate ruby class.
 * When false, results are returned as +Strings+.
 *
 */
VALUE
pgconn_s_translate_results_set( self, fact)
    VALUE self, fact;
{
    translate_results = RTEST( fact) ? 1 : 0;
    return Qnil;
}


VALUE
pgconn_alloc( klass)
    VALUE klass;
{
    return Data_Wrap_Struct( klass, 0, &PQfinish, NULL);
}

/*
 * Document-method: new
 *
 * call-seq:
 *     Pg::Conn.new( connection_hash, ...) -> Pg::Conn
 *     Pg::Conn.new( connection_string) -> Pg::Conn
 *     Pg::Conn.new( host, port, options, tty, dbname, login, passwd) ->  Pg::Conn
 *
 *  _host_::     server hostname
 *  _port_::     server port number
 *  _options_::  backend options (String)
 *  _tty_::      tty to print backend debug message <i>(ignored in newer
 *                  versions of PostgreSQL)</i> (String)
 *  _dbname_::   connecting database name
 *  _login_::    login user name
 *  _passwd_::   login password
 *
 *  On failure, it raises a Pg::Error exception.
 */
VALUE
pgconn_init( argc, argv, self)
    int argc;
    VALUE *argv;
    VALUE self;
{
    VALUE args;
    PGconn *conn = NULL;

    rb_scan_args( argc, argv, "0*", &args);
    if (RARRAY( args)->len == 1)
        conn = try_connectdb( rb_ary_entry( args, 0));
    else {
        if (RARRAY( args)->len == 0 ||
                !NIL_P( rb_check_hash_type( rb_ary_entry( args, 0)))) {
            int i;
            VALUE arg, hsh;

            arg = rb_hash_new();
            for (i = 0; i < RARRAY( args)->len; ++i) {
                hsh = rb_ary_entry( args, i);
                if (!NIL_P( hsh)) {
                    rb_funcall( arg, rb_intern( "update"), 1, hsh);
                }
            }
            conn = try_connectdb( arg);
        }
    }
    if (conn == NULL) {
        conn = try_setdbLogin( args);
    }

    if (PQstatus( conn) == CONNECTION_BAD)
        pg_raise_conn( conn);

    Data_Set_Struct( self, conn);
    return self;
}

/*
 * call-seq:
 *    conn.close()
 *
 * Closes the backend connection.
 */
VALUE
pgconn_close( obj)
    VALUE obj;
{
    PQfinish( get_pgconn( obj));
    DATA_PTR( obj) = NULL;
    return Qnil;
}

/*
 * call-seq:
 *    conn.reset()
 *
 * Resets the backend connection. This method closes the backend connection
 * and tries to re-connect.
 */
VALUE
pgconn_reset( obj)
    VALUE obj;
{
    PQreset( get_pgconn( obj));
    return obj;
}

/*
 * call-seq:
 *  conn.protocol_version -> Integer
 *
 * The 3.0 protocol will normally be used when communicating with PostgreSQL
 * 7.4 or later servers; pre-7.4 servers support only protocol 2.0. (Protocol
 * 1.0 is obsolete and not supported by libpq.)
 */
VALUE
pgconn_protocol_version( obj)
    VALUE obj;
{
    return INT2NUM( PQprotocolVersion( get_pgconn( obj)));
}

/*
 * call-seq:
 *   conn.server_version -> Integer
 *
 * The number is formed by converting the major, minor, and revision numbers
 * into two-decimal-digit numbers and appending them together. For example,
 * version 7.4.2 will be returned as 70402, and version 8.1 will be returned
 * as 80100 (leading zeroes are not shown). Zero is returned if the connection
 * is bad.
 */
VALUE
pgconn_server_version( obj)
    VALUE obj;
{
    return INT2NUM( PQserverVersion( get_pgconn( obj)));
}

/*
 * call-seq:
 *    conn.db()
 *
 * Returns the connected database name.
 */
VALUE
pgconn_db( obj)
    VALUE obj;
{
    char *db = PQdb( get_pgconn( obj));
    return db == NULL ? Qnil : rb_tainted_str_new2( db);
}

/*
 * call-seq:
 *    conn.host()
 *
 * Returns the connected server name.
 */
VALUE
pgconn_host( obj)
    VALUE obj;
{
    char *host = PQhost( get_pgconn( obj));
    return host == NULL ? Qnil : rb_tainted_str_new2( host);
}

/*
 * call-seq:
 *    conn.options()
 *
 * Returns backend option string.
 */
VALUE
pgconn_options( obj)
    VALUE obj;
{
    char *options = PQoptions( get_pgconn( obj));
    return options == NULL ? Qnil : rb_tainted_str_new2( options);
}

/*
 * call-seq:
 *    conn.port()
 *
 * Returns the connected server port number.
 */
VALUE
pgconn_port( obj)
    VALUE obj;
{
    char* port = PQport( get_pgconn( obj));
    return port == NULL ? Qnil : INT2NUM( atol( port));
}

/*
 * call-seq:
 *    conn.tty()
 *
 * Returns the connected pgtty.
 */
VALUE
pgconn_tty( obj)
    VALUE obj;
{
    char *tty = PQtty( get_pgconn( obj));
    return tty == NULL ? Qnil : rb_tainted_str_new2( tty);
}

/*
 * call-seq:
 *    conn.user()
 *
 * Returns the authenticated user name.
 */
VALUE
pgconn_user( obj)
    VALUE obj;
{
    char *user = PQuser( get_pgconn( obj));
    return user == NULL ? Qnil : rb_tainted_str_new2( user);
}

/*
 * call-seq:
 *    conn.status()
 *
 * This may return the values +CONNECTION_OK+ or +CONNECTION_BAD+.
 */
VALUE
pgconn_status( obj)
    VALUE obj;
{
    return INT2NUM( PQstatus( get_pgconn( obj)));
}

/*
 * call-seq:
 *    conn.error()
 *
 * Returns the error message about connection.
 */
VALUE
pgconn_error( obj)
    VALUE obj;
{
    char *error = PQerrorMessage( get_pgconn( obj));
    return error != NULL ? rb_tainted_str_new2( error) : Qnil;
}




/*
 * call-seq:
 *    conn.lo_import( file) -> oid
 *
 * Import a file to a large object. Returns an oid on success. On
 * failure, it raises a Pg::Error exception.
 */
VALUE
pgconn_loimport( obj, filename)
    VALUE obj, filename;
{
    Oid lo_oid;
    PGconn *conn;

    Check_Type( filename, T_STRING);

    conn = get_pgconn( obj);
    lo_oid = lo_import( conn, STR2CSTR( filename));
    if (lo_oid == 0)
        pg_raise_exec( conn);
    return INT2NUM( lo_oid);
}

/*
 * call-seq:
 *    conn.lo_export( oid, file )
 *
 * Saves a large object of _oid_ to a _file_.
 */
VALUE
pgconn_loexport( obj, lo_oid, filename)
    VALUE obj, lo_oid, filename;
{
    int oid;
    PGconn *conn;

    Check_Type( filename, T_STRING);

    oid = NUM2INT( lo_oid);
    if (oid < 0)
        rb_raise( rb_ePGError, "invalid large object oid %d", oid);

    conn = get_pgconn( obj);
    if (!lo_export( conn, oid, STR2CSTR( filename)))
        pg_raise_exec( conn);
    return Qnil;
}


/*
 * call-seq:
 *    conn.lo_create( [mode] ) -> Pg::Large
 *    conn.lo_create( [mode] ) { |pglarge| ... } -> oid
 *
 * Returns a Pg::Large instance on success. On failure, it raises Pg::Error
 * exception. <i>(See #lo_open for information on _mode_.)</i>
 *
 * If a block is given, the blocks result is returned.
 *
 */
VALUE
pgconn_locreate( argc, argv, obj)
    int argc;
    VALUE *argv;
    VALUE obj;
{
    VALUE nmode;

    rb_scan_args( argc, argv, "01", &nmode);
    return locreate_pgconn( get_pgconn( obj), nmode);
}

/*
 * call-seq:
 *    conn.lo_open( oid, [mode] ) -> Pg::Large
 *    conn.lo_open( oid, [mode] ) { |pglarge| ... } -> obj
 *
 * Open a large object of _oid_. Returns a Pg::Large instance on success.
 * The _mode_ argument specifies the mode for the opened large object,
 * which is either +INV_READ+, or +INV_WRITE+.
 * * If _mode_ On failure, it raises a Pg::Error exception.
 * * If _mode_ is omitted, the default is +INV_READ+.
 *
 * If a block is given, the blocks result is returned.
 *
 */
VALUE
pgconn_loopen( argc, argv, obj)
    int argc;
    VALUE *argv;
    VALUE obj;
{
    VALUE nmode, objid;

    rb_scan_args( argc, argv, "11", &objid, &nmode);
    return loopen_pgconn( get_pgconn( obj), objid, nmode);
}

/*
 * call-seq:
 *    conn.lo_unlink( oid )
 *
 * Unlinks (deletes) the postgres large object of _oid_.
 */
VALUE
pgconn_lounlink( obj, lo_oid)
    VALUE obj, lo_oid;
{
    int oid;
    int result;

    oid = NUM2INT( lo_oid);
    if (oid < 0)
        rb_raise( rb_ePGError, "invalid oid %d", oid);
    result = lo_unlink( get_pgconn( obj), oid);
    if (result < 0)
        rb_raise( rb_ePGError, "unlink of oid %d failed", oid);
    return Qnil;
}



/********************************************************************
 *
 * Document-class: Pg::Conn
 *
 * The class to access PostgreSQL database.
 *
 * For example, to send query to the database on the localhost:
 *
 *    require "pgsql"
 *    conn = Pg::Conn.open "dbname" => "test1"
 *    res = conn.exec "select * from mytable;"
 *
 * See the Pg::Result class for information on working with the results of a
 * query.
 */

void init_pg_conn( void)
{
    rb_require( "date");
    rb_require( "time");
    rb_cDate       = RUBY_CLASS( "Date");
    rb_cDateTime   = RUBY_CLASS( "DateTime");
    rb_cRational   = RUBY_CLASS( "Rational");

    rb_cPGConn = rb_define_class_under( rb_mPg, "Conn", rb_cObject);

    rb_define_alloc_func( rb_cPGConn, pgconn_alloc);
    rb_define_singleton_method( rb_cPGConn, "connect", pgconn_s_connect, -1);
    rb_define_singleton_alias( rb_cPGConn, "setdb", "connect");
    rb_define_singleton_alias( rb_cPGConn, "setdblogin", "connect");
    rb_define_singleton_alias( rb_cPGConn, "open", "connect");

    rb_define_singleton_method( rb_cPGConn, "escape", pgconn_s_escape, 1);
    rb_define_singleton_method( rb_cPGConn, "quote", pgconn_s_quote, 1);
    rb_define_singleton_alias( rb_cPGConn, "format", "quote");
    rb_define_singleton_method( rb_cPGConn, "escape_bytea",
                                                    pgconn_s_escape_bytea, 1);
    rb_define_singleton_method( rb_cPGConn, "unescape_bytea",
                                                  pgconn_s_unescape_bytea, 1);
    rb_define_singleton_method( rb_cPGConn, "translate_results=",
                                           pgconn_s_translate_results_set, 1);

    rb_define_const( rb_cPGConn, "CONNECTION_OK",  INT2FIX( CONNECTION_OK));
    rb_define_const( rb_cPGConn, "CONNECTION_BAD", INT2FIX( CONNECTION_BAD));

    rb_define_method( rb_cPGConn, "initialize", pgconn_init, -1);
    rb_define_method( rb_cPGConn, "close", pgconn_close, 0);
    rb_define_alias( rb_cPGConn, "finish", "close");
    rb_define_method( rb_cPGConn, "reset", pgconn_reset, 0);
    rb_define_method( rb_cPGConn, "protocol_version",
                                                   pgconn_protocol_version, 0);
    rb_define_method( rb_cPGConn, "server_version", pgconn_server_version, 0);
    rb_define_method( rb_cPGConn, "db", pgconn_db, 0);
    rb_define_alias( rb_cPGConn, "dbname", "db");
    rb_define_method( rb_cPGConn, "host", pgconn_host, 0);
    rb_define_method( rb_cPGConn, "options", pgconn_options, 0);
    rb_define_method( rb_cPGConn, "port", pgconn_port, 0);
    rb_define_method( rb_cPGConn, "tty", pgconn_tty, 0);
    rb_define_method( rb_cPGConn, "user", pgconn_user, 0);
    rb_define_method( rb_cPGConn, "status", pgconn_status, 0);
    rb_define_method( rb_cPGConn, "error", pgconn_error, 0);

    rb_define_method( rb_cPGConn, "trace", pgconn_trace, 1);
    rb_define_method( rb_cPGConn, "untrace", pgconn_untrace, 0);
    rb_define_method( rb_cPGConn, "exec", pgconn_exec, -1);
    rb_define_method( rb_cPGConn, "query", pgconn_query, -1);
    rb_define_method( rb_cPGConn, "select_one", pgconn_select_one, -1);
    rb_define_method( rb_cPGConn, "select_value", pgconn_select_value, -1);
    rb_define_method( rb_cPGConn, "select_values", pgconn_select_values, -1);
    rb_define_method( rb_cPGConn, "async_exec", pgconn_async_exec, 1);
    rb_define_method( rb_cPGConn, "async_query", pgconn_async_query, 1);
    rb_define_method( rb_cPGConn, "get_notify", pgconn_get_notify, 0);
    rb_define_method( rb_cPGConn, "insert_table", pgconn_insert_table, 2);
    rb_define_method( rb_cPGConn, "transaction", pgconn_transaction, -1);
    rb_define_method( rb_cPGConn, "subtransaction", pgconn_subtransaction, -1);
    rb_define_alias( rb_cPGConn, "savepoint", "subtransaction");
    rb_define_method( rb_cPGConn, "putline", pgconn_putline, 1);
    rb_define_method( rb_cPGConn, "getline", pgconn_getline, 0);
    rb_define_method( rb_cPGConn, "endcopy", pgconn_endcopy, 0);
    rb_define_method( rb_cPGConn, "on_notice", pgconn_on_notice, 0);
    rb_define_method( rb_cPGConn, "transaction_status",
                                                 pgconn_transaction_status, 0);
    rb_define_method( rb_cPGConn, "quote", pgconn_quote, 1);

    rb_define_method( rb_cPGConn, "client_encoding", pgconn_client_encoding, 0);
    rb_define_method( rb_cPGConn, "set_client_encoding",
                                               pgconn_set_client_encoding, 1);


    rb_define_method( rb_cPGConn, "lo_import", pgconn_loimport, 1);
    rb_define_alias( rb_cPGConn, "loimport", "lo_import");
    rb_define_method( rb_cPGConn, "lo_export", pgconn_loexport, 2);
    rb_define_alias( rb_cPGConn, "loexport", "lo_export");
    rb_define_method( rb_cPGConn, "lo_unlink", pgconn_lounlink, 1);
    rb_define_alias( rb_cPGConn, "lounlink", "lo_unlink");
    rb_define_method( rb_cPGConn, "lo_create", pgconn_locreate, -1);
    rb_define_alias( rb_cPGConn, "locreate", "lo_create");
    rb_define_method( rb_cPGConn, "lo_open", pgconn_loopen, -1);
    rb_define_alias( rb_cPGConn, "loopen", "lo_open");
}

