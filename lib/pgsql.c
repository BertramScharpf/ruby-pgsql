/*
 *  pgsql.c  --  Pg::Xxx classes
 */

#include "pgsql.h"

#include "large.h"
#include "row.h"
#include "result.h"

#include <st.h>
#include <intern.h>


static VALUE rb_cBigDecimal;
static VALUE rb_cRational;
static VALUE rb_cDate;
static VALUE rb_cDateTime;

static VALUE rb_cPGConn;

static ID id_new;
static ID id_on_notice;
static ID id_gsub;
static ID id_gsub_bang;

static VALUE pg_escape_regex;
static VALUE pg_escape_str;

static int translate_results = 1;

static VALUE pgconn_alloc( VALUE klass);
static PGconn *try_connectdb( VALUE arg);
static PGconn *try_setdbLogin( VALUE args);
static VALUE pgconn_s_translate_results_set( VALUE self, VALUE fact);
static VALUE format_single_element( VALUE obj);
static VALUE pgconn_s_format( VALUE self, VALUE obj);
static VALUE format_array_element( VALUE obj);
static VALUE pgconn_s_quote( VALUE self, VALUE obj);
static int build_key_value_string_i( VALUE key, VALUE value, VALUE result);
static VALUE pgconn_s_escape( VALUE self, VALUE string);
static VALUE pgconn_s_escape_bytea( VALUE self, VALUE obj);
static VALUE pgconn_s_unescape_bytea( VALUE self, VALUE obj);
static VALUE pgconn_init( int argc, VALUE *argv, VALUE self);
static PGconn *get_pgconn( VALUE obj);
static VALUE pgconn_s_connect( int argc, VALUE *argv, VALUE klass);
static VALUE pgconn_close( VALUE obj );
static VALUE pgconn_reset( VALUE obj);
static VALUE yield_or_return_result( VALUE res);

static VALUE pgconn_exec( int argc, VALUE *argv, VALUE obj);
static VALUE pgconn_async_exec( VALUE obj, VALUE str);
static VALUE pgconn_query( int argc, VALUE *argv, VALUE obj);
static VALUE pgconn_async_query( VALUE obj, VALUE str);
static VALUE pgconn_get_notify( VALUE obj);

static void free_pgconn( PGconn *ptr);
static VALUE pgconn_insert_table( VALUE obj, VALUE table, VALUE values);
static VALUE rescue_transaction( VALUE obj);
static VALUE yield_transaction( VALUE obj);
static VALUE pgconn_transaction( int argc, VALUE *argv, VALUE obj);
static VALUE rescue_subtransaction( VALUE obj);
static VALUE yield_subtransaction( VALUE obj);
static VALUE pgconn_subtransaction( int argc, VALUE *argv, VALUE obj);

static VALUE pgconn_putline( VALUE obj, VALUE str);
static VALUE pgconn_getline( VALUE obj);
static VALUE pgconn_endcopy( VALUE obj);
static void notice_proxy( void *self, const char *message);
static VALUE pgconn_on_notice( VALUE self);
static VALUE pgconn_host( VALUE obj);
static VALUE pgconn_port( VALUE obj);
static VALUE pgconn_db( VALUE obj);
static VALUE pgconn_options( VALUE obj);
static VALUE pgconn_tty( VALUE obj);
static VALUE pgconn_user( VALUE obj);
static VALUE pgconn_status( VALUE obj);
static VALUE pgconn_error( VALUE obj);
static VALUE pgconn_trace( VALUE obj, VALUE port);
static VALUE pgconn_untrace( VALUE obj);
static VALUE pgconn_transaction_status( VALUE obj);

static VALUE pgconn_protocol_version( VALUE obj);
static VALUE pgconn_server_version( VALUE obj);
static VALUE pgconn_quote( VALUE obj, VALUE value);
static VALUE pgconn_lastval( VALUE obj);
static VALUE pgconn_client_encoding( VALUE obj);
static VALUE pgconn_set_client_encoding( VALUE obj, VALUE str);

static int has_numeric_scale( int typmod);

static VALUE pgconn_select_one( int argc, VALUE *argv, VALUE self);
static VALUE pgconn_select_value( int argc, VALUE *argv, VALUE self);
static VALUE pgconn_select_values( int argc, VALUE *argv, VALUE self);


static VALUE pgconn_loimport( VALUE obj, VALUE filename);
static VALUE pgconn_loexport( VALUE obj, VALUE lo_oid, VALUE filename);

static VALUE pgconn_locreate( int argc, VALUE *argv, VALUE obj);
static VALUE pgconn_loopen( int argc, VALUE *argv, VALUE obj);
static VALUE pgconn_lounlink( VALUE obj, VALUE lo_oid);




static PGresult *pg_pqexec( PGconn *conn, const char *cmd);



PGresult *pg_pqexec( PGconn *conn, const char *cmd)
{
    PGresult *result;

    result = PQexec( conn, cmd);
    pg_checkresult( conn, result);
    return result;
}




VALUE
pgconn_alloc( klass)
    VALUE klass;
{
    return Data_Wrap_Struct( klass, 0, &free_pgconn, NULL);
}


PGconn *
try_connectdb( arg)
    VALUE arg;
{
    VALUE conninfo;

    if        (!NIL_P( conninfo = rb_check_string_type( arg))) {
        ;
    } else if (!NIL_P( conninfo = rb_check_hash_type( arg))) {
        VALUE key_values = rb_ary_new2( RHASH( conninfo)->tbl->num_entries);
        rb_hash_foreach( conninfo, build_key_value_string_i, key_values);
        conninfo = rb_ary_join( key_values, rb_str_new2( " "));
    } else {
        return NULL;
    }
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
format_single_element( obj)
    VALUE obj;
{
    VALUE result;
    int tainted;
    long i;

    switch (TYPE( obj)) {
    case T_STRING:
        return obj;

    case T_TRUE:
    case T_FALSE:
    case T_FIXNUM:
    case T_BIGNUM:
    case T_FLOAT:
        return rb_obj_as_string( obj);

    case T_NIL:
        return rb_str_new2( "NULL");

    case T_ARRAY:
        result = rb_str_buf_new2( "'{");
        tainted = OBJ_TAINTED( obj);
        for (i = 0; i < RARRAY( obj)->len; i++) {
            VALUE element = format_array_element( RARRAY( obj)->ptr[i]);
            if (OBJ_TAINTED( RARRAY( obj)->ptr[i])) tainted = Qtrue;
            if (i > 0) rb_str_buf_cat2( result, ",");
            rb_str_buf_append( result, element);
        }
        rb_str_buf_cat2( result, "}'");
        if (tainted) OBJ_TAINT( result);
        return result;

    default:
        if (CLASS_OF( obj) == rb_cBigDecimal) {
            return rb_funcall( obj, rb_intern( "to_s"), 1, rb_str_new2( "F"));
        } else {
            return Qundef;
        }
    }
}

VALUE
pgconn_s_format( self, obj)
    VALUE self;
    VALUE obj;
{
    VALUE result;

    result = format_single_element( obj);
    if (result == Qundef) {
        result = rb_obj_as_string( obj);
    }
    return result;
}

VALUE
format_array_element( obj)
    VALUE obj;
{
    if (TYPE( obj) == T_STRING) {
        obj = rb_funcall( obj, id_gsub,
                    2, rb_reg_new( "(?=[\\\\\"])", 9, 0), rb_str_new2( "\\"));
        return rb_funcall( obj, id_gsub_bang,
                    2, rb_reg_new( "^|$", 3, 0), rb_str_new2( "\""));
    }
    else {
        return pgconn_s_format( rb_cPGConn, obj);
    }
}

/*
 * call-seq:
 *    Pg::Conn.quote( obj )
 *    Pg::Conn.quote( obj ) { |obj| ... }
 *    Pg::Conn.format( obj )
 *    Pg::Conn.format( obj ) { |obj| ... }
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
 * If _obj_ is not one of the recognized classes andno block is supplied,
 * a Pg::Error is raised.
 */
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
 * call-seq:
 *    Pg::Conn.escape( str)
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
 * [http://www.postgresql.org/docs/current/interactive/libpq-exec.html#LIBPQ-EXEC-ESCAPE-BYTEA]
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
    if (RARRAY( args)->len == 1) {
        conn = try_connectdb( rb_ary_entry( args, 0));
    } else {
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
    VALUE pgconn = rb_class_new_instance( argc, argv, klass);

    if (rb_block_given_p()) {
        return rb_ensure( rb_yield, pgconn, pgconn_close, pgconn);
    }

    return pgconn;
}


/*
 * call-seq:
 *    conn.close
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
 * Resets the backend connection. This method closes the backend  connection
 * and tries to re-connect.
 */
VALUE
pgconn_reset( obj)
    VALUE obj;
{
    PQreset( get_pgconn( obj));
    return obj;
}

VALUE
yield_or_return_result( result)
    VALUE result;
{
    return RTEST( rb_block_given_p()) ?
        rb_ensure( rb_yield, result, pgresult_clear, result) : result;
}


/*
 * call-seq:
 *    conn.exec( sql, *bind_values)
 *
 * Sends SQL query request specified by _sql_ to the PostgreSQL.
 * Returns a Pg::Result instance on success.
 * On failure, it raises a Pg::Error exception.
 *
 * +bind_values+ represents values for the PostgreSQL bind parameters found in
 * the +sql+.  PostgreSQL bind parameters are presented as $1, $1, $2, etc.
 */
VALUE
pgconn_exec( argc, argv, obj)
    int argc;
    VALUE *argv;
    VALUE obj;
{
    PGconn *conn = get_pgconn( obj);
    PGresult *result = NULL;
    VALUE command, params;

    rb_scan_args( argc, argv, "1*", &command, &params);

    Check_Type( command, T_STRING);

    if (RARRAY( params)->len <= 0) {
        result = pg_pqexec( conn, STR2CSTR( command));
    } else {
        int len = RARRAY( params)->len;
        int i;
        VALUE* ptr = RARRAY( params)->ptr;
        const char **values = ALLOCA_N( const char *, len);
        VALUE formatted;
        for (i = 0; i < len; i++, ptr++) {
            if (*ptr == Qnil) {
                values[i] = NULL;
            }
            else {
                formatted = pgconn_s_format( rb_cPGConn, *ptr);
                values[i] = STR2CSTR( formatted);
            }
        }
        result = PQexecParams( conn, STR2CSTR( command), len,
                                NULL, values, NULL, NULL, 0);
        pg_checkresult( conn, result);
    }

    return yield_or_return_result( pgresult_new( conn, result));
}

/*
 * call-seq:
 *    conn.async_exec( sql )
 *
 * Sends an asyncrhonous SQL query request specified by _sql_ to the
 * PostgreSQL server.
 * Returns a Pg::Result instance on success.
 * On failure, it raises a Pg::Error exception.
 */
VALUE
pgconn_async_exec( obj, str)
    VALUE obj, str;
{
    PGconn *conn = get_pgconn( obj);
    PGresult *result;
    int cs;
    int ret;
    fd_set rset;

    Check_Type( str, T_STRING);

    while ((result = PQgetResult( conn)))
        PQclear( result);
    if (!PQsendQuery( conn, RSTRING( str)->ptr))
        pg_raise_exec( conn);
    cs = PQsocket( conn);
    for(;;) {
        FD_ZERO(&rset);
        FD_SET( cs, &rset);
        ret = rb_thread_select( cs + 1, &rset, NULL, NULL, NULL);
        if (ret < 0)
            rb_sys_fail( 0);
        if (ret == 0)
            continue;
        if (PQconsumeInput( conn) == 0)
            pg_raise_exec( conn);
        if (PQisBusy( conn) == 0)
            break;
    }

    result = PQgetResult( conn);
    pg_checkresult( conn, result);
    return yield_or_return_result( pgresult_new( conn, result));
}

/*
 * call-seq:
 *    conn.query( sql, *bind_values)
 *
 * Sends SQL query request specified by _sql_ to the PostgreSQL.
 * Returns an Array as the resulting tuple on success.
 * On failure, it returns +nil+, and the error details can be obtained by
 * #error.
 *
 * +bind_values+ represents values for the PostgreSQL bind parameters found in
 * the +sql+.  PostgreSQL bind parameters are presented as $1, $1, $2, etc.
 */
VALUE
pgconn_query( argc, argv, obj)
    int argc;
    VALUE *argv;
    VALUE obj;
{
    VALUE result;

    result = rb_funcall2( obj, rb_intern( "exec"), argc, argv);
    return pgresult_result_with_clear( result);
}

/*
 * call-seq:
 *    conn.async_query( sql)
 *
 * Sends an asynchronous SQL query request specified by _sql_ to the
 * PostgreSQL server.
 * Returns an Array as the resulting tuple on success.
 * On failure, it returns +nil+, and the error details can be obtained by
 * #error.
 */
VALUE
pgconn_async_query( obj, str)
    VALUE obj, str;
{
    VALUE result;

    result = rb_funcall( obj, rb_intern( "async_exec"), 1, str);
    return pgresult_result_with_clear( result);
}

/*
 * call-seq:
 *    conn.get_notify()
 *
 * Returns an array of the unprocessed notifiers.
 * If there is no unprocessed notifier, it returns +nil+.
 */
VALUE
pgconn_get_notify( obj)
    VALUE obj;
{
    PGconn* conn = get_pgconn( obj);
    PGnotify *notify;
    VALUE ary;

    if (PQconsumeInput( conn) == 0) {
        pg_raise_exec( conn);
    }
    /* gets notify and builds result */
    notify = PQnotifies( conn);
    if (notify == NULL) {
        /* there are no unhandled notifications */
        return Qnil;
    }
    ary = rb_ary_new3( 2, rb_tainted_str_new2( notify->relname),
                            INT2NUM( notify->be_pid));
    PQfreemem( notify);

    /* returns result */
    return ary;
}

void
free_pgconn( ptr)
    PGconn *ptr;
{
    PQfinish( ptr);
}

/*
 * call-seq:
 *    conn.insert_table( table, values )
 *
 * Inserts contents of the _values_ Array into the _table_.
 */
VALUE
pgconn_insert_table( obj, table, values)
    VALUE obj, table, values;
{
    PGconn *conn = get_pgconn( obj);
    PGresult *result;
    VALUE s, buffer;
    int i, j;
    int res = 0;

    Check_Type( table, T_STRING);
    Check_Type( values, T_ARRAY);
    i = RARRAY( values)->len;
    while (i--) {
        if (TYPE( RARRAY( RARRAY( values)->ptr[i])) != T_ARRAY) {
            rb_raise( rb_ePGError,
                     "second arg must contain some kind of arrays.");
        }
    }

    buffer = rb_str_new( 0, RSTRING( table)->len + 17 + 1);
    /* starts query */
    snprintf( RSTRING( buffer)->ptr, RSTRING( buffer)->len,
                "copy %s from stdin ", STR2CSTR( table));

    result = pg_pqexec( conn, STR2CSTR( buffer));
    PQclear( result);

    for (i = 0; i < RARRAY( values)->len; i++) {
        struct RArray *row = RARRAY( RARRAY( values)->ptr[i]);
        buffer = rb_tainted_str_new( 0, 0);
        for (j = 0; j < row->len; j++) {
            if (j > 0) rb_str_cat( buffer, "\t", 1);
            if (NIL_P( row->ptr[j])) {
                rb_str_cat( buffer, "\\N", 2);
            } else {
                s = rb_obj_as_string( row->ptr[j]);
                rb_funcall( s, id_gsub_bang, 2, pg_escape_regex, pg_escape_str);
                rb_str_cat( buffer, STR2CSTR( s), RSTRING( s)->len);
            }
        }
        rb_str_cat( buffer, "\n\0", 2);
        /* sends data */
        PQputline( conn, STR2CSTR( buffer));
    }
    PQputline( conn, "\\.\n");
    res = PQendcopy( conn);

    return obj;
}



VALUE
rescue_transaction( obj)
    VALUE obj;
{
    pg_pqexec( get_pgconn( obj), "rollback;");
    rb_exc_raise( ruby_errinfo);
    return Qnil;
}

VALUE
yield_transaction( obj)
    VALUE obj;
{
    VALUE r;

    r = rb_yield( obj);
    pg_pqexec( get_pgconn( obj), "commit;");
    return r;
}

/*
 * call-seq:
 *    conn.transaction( ser = nil, ro = nil) { |conn| ... }
 *
 * Open and close a transaction block. The isolation level will be
 * 'serializable' if +ser+ is true, else 'repeatable read'.
 * +ro+ means 'read only'.
 *
 * (In C++ terms, +ro+ is const, and +ser+ is not volatile.)
 *
 */
VALUE
pgconn_transaction( argc, argv, self)
    int argc;
    VALUE *argv;
    VALUE self;
{
    VALUE ser, ro;
    VALUE cmd;
    int p;

    rb_scan_args( argc, argv, "02", &ser, &ro);
    cmd = rb_str_buf_new2( "begin");
    p = 0;
    if (!NIL_P(ser)) {
        rb_str_buf_cat2( cmd, " isolation level ");
        rb_str_buf_cat2( cmd, (RTEST(ser) ? "serializable" : "read committed"));
        p++;
    }
    if (!NIL_P(ro)) {
        if (p) rb_str_buf_cat2( cmd, ",");
        rb_str_buf_cat2( cmd, " read ");
        rb_str_buf_cat2( cmd, (RTEST(ro)  ? "only" : "write"));
    }
    rb_str_buf_cat2( cmd, ";");
    pg_pqexec( get_pgconn( self), STR2CSTR(cmd));
    return rb_rescue( yield_transaction, self, rescue_transaction, self);
}




VALUE
rescue_subtransaction( ary)
    VALUE ary;
{
    VALUE cmd;

    cmd = rb_str_buf_new2( "rollback to savepoint ");
    rb_str_buf_append( cmd, rb_ary_entry( ary, 1));
    rb_str_buf_cat2( cmd, ";");
    pg_pqexec( get_pgconn( rb_ary_entry( ary, 0)), STR2CSTR(cmd));

    rb_exc_raise( ruby_errinfo);
    return Qnil;
}

VALUE
yield_subtransaction( ary)
    VALUE ary;
{
    VALUE r, cmd;

    r = rb_yield( ary);

    cmd = rb_str_buf_new2( "release savepoint ");
    rb_str_buf_append( cmd, rb_ary_entry( ary, 1));
    rb_str_buf_cat2( cmd, ";");
    pg_pqexec( get_pgconn( rb_ary_entry( ary, 0)), STR2CSTR(cmd));

    return r;
}

/*
 * call-seq:
 *    conn.subtransaction( nam, *args) { |conn,sp| ... }
 *
 * Open and close a transaction savepoint. The savepoints name +nam+ may
 * contain % directives that will be expanded by +args+.
 */
VALUE
pgconn_subtransaction( argc, argv, self)
    int argc;
    VALUE *argv;
    VALUE self;
{
    VALUE sp, par, cmd, ya;

    if (rb_scan_args( argc, argv, "1*", &sp, &par) > 1)
        sp = rb_str_format(RARRAY_LEN(par), RARRAY_PTR(par), sp);
    rb_str_freeze( sp);

    cmd = rb_str_buf_new2( "savepoint ");
    rb_str_buf_append( cmd, sp);
    rb_str_buf_cat2( cmd, ";");
    pg_pqexec( get_pgconn( self), STR2CSTR(cmd));

    ya = rb_ary_new3( 2, self, sp);
    return rb_rescue( yield_subtransaction, ya, rescue_subtransaction, ya);
}



/*
 * call-seq:
 *    conn.putline()
 *
 * Sends the string to the backend server.
 * Users must send a single "." to denote the end of data transmission.
 */
VALUE
pgconn_putline( obj, str)
    VALUE obj, str;
{
    Check_Type( str, T_STRING);
    PQputline( get_pgconn( obj), STR2CSTR( str));
    return obj;
}

/*
 * call-seq:
 *    conn.getline()
 *
 * Reads a line from the backend server into internal buffer.
 * Returns +nil+ for EOF, +0+ for success, +1+ for buffer overflowed.
 * You need to ensure single "." from backend to confirm  transmission
 * completion.
 * The sample program <tt>psql.rb</tt> (see source for postgres) treats this
 * copy protocol right.
 */
VALUE
pgconn_getline( obj)
    VALUE obj;
{
    PGconn *conn = get_pgconn( obj);
    VALUE str;
    long size = BUFSIZ;
    long bytes = 0;
    int  ret;

    str = rb_tainted_str_new( 0, size);

    for (;;) {
        ret = PQgetline( conn, RSTRING( str)->ptr + bytes, size - bytes);
        switch (ret) {
        case EOF:
            return Qnil;
        case 0:
            rb_str_resize( str, strlen( STR2CSTR( str)));
            return str;
        }
        bytes += BUFSIZ;
        size += BUFSIZ;
        rb_str_resize( str, size);
    }
    return Qnil;
}

/*
 * call-seq:
 *    conn.endcopy()
 *
 * Waits until the backend completes the copying.
 * You should call this method after #putline or #getline.
 * Returns +nil+ on success; raises an exception otherwise.
 */
VALUE
pgconn_endcopy( obj)
    VALUE obj;
{
    if (PQendcopy( get_pgconn( obj)) == 1) {
        rb_raise( rb_ePGError, "cannot complete copying");
    }
    return Qnil;
}

void
notice_proxy( self, message)
    void *self;
    const char *message;
{
    VALUE block;
    if ((block = rb_ivar_get( (VALUE) self, id_on_notice)) != Qnil) {
        rb_funcall( block, rb_intern( "call"), 1, rb_str_new2( message));
    }
}

/*
 * call-seq:
 *   conn.on_notice {|message| ... }
 *
 * Notice and warning messages generated by the server are not returned
 * by the query execution functions, since they do not imply failure of
 * the query. Instead they are passed to a notice handling function, and
 * execution continues normally after the handler returns. The default
 * notice handling function prints the message on <tt>stderr</tt>, but the
 * application can override this behavior by supplying its own handling
 * function.
 */
VALUE
pgconn_on_notice( self)
    VALUE self;
{
    VALUE block = rb_block_proc();
    PGconn *conn = get_pgconn( self);
    if (PQsetNoticeProcessor( conn, NULL, NULL) != notice_proxy) {
        PQsetNoticeProcessor( conn, notice_proxy, (void *) self);
    }
    rb_ivar_set( self, id_on_notice, block);
    return self;
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
    if (!host) return Qnil;
    return rb_tainted_str_new2( host);
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
    return INT2NUM( atol( port));
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
    if (!db) return Qnil;
    return rb_tainted_str_new2( db);
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
    if (!options) return Qnil;
    return rb_tainted_str_new2( options);
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
    if (!tty) return Qnil;
    return rb_tainted_str_new2( tty);
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
    if (!user) return Qnil;
    return rb_tainted_str_new2( user);
}

/*
 * call-seq:
 *    conn.status()
 *
 * MISSING: documentation
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
 *    conn.trace( port )
 *
 * Enables tracing message passing between backend.
 * The trace message will be written to the _port_ object,
 * which is an instance of the class +File+.
 */
VALUE
pgconn_trace( obj, port)
    VALUE obj, port;
{
    OpenFile* fp;

    Check_Type( port, T_FILE);
    GetOpenFile( port, fp);

    PQtrace( get_pgconn( obj), fp->f2?fp->f2:fp->f);

    return obj;
}

/*
 * call-seq:
 *    conn.untrace()
 *
 * Disables the message tracing.
 */
VALUE
pgconn_untrace( obj)
    VALUE obj;
{
    PQuntrace( get_pgconn( obj));
    return obj;
}

/*
 * call-seq:
 *    conn.transaction_status()
 *
 * returns one of the following statuses:
 *   PQTRANS_IDLE    = 0 (connection idle)
 *   PQTRANS_ACTIVE  = 1 (command in progress)
 *   PQTRANS_INTRANS = 2 (idle, within transaction block)
 *   PQTRANS_INERROR = 3 (idle, within failed transaction)
 *   PQTRANS_UNKNOWN = 4 (cannot determine status)
 *
 * See the PostgreSQL documentation on PQtransactionStatus
 * [http://www.postgresql.org/docs/current/interactive/libpq-status.html#AEN24919]
 * for more information.
 */
VALUE
pgconn_transaction_status( obj)
    VALUE obj;
{
    return INT2NUM( PQtransactionStatus( get_pgconn( obj)));
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
 *    conn.quote( obj)
 *    conn.quote( obj) { |obj| ... }
 *
 * A shortcut for +Pg::Conn.quote+. See there for further explanation.
 */
VALUE
pgconn_quote( obj, value)
    VALUE obj, value;
{
    return pgconn_s_quote( rb_cPGConn, value);
}


/*
 * call-seq:
 *    conn.client_encoding() -> String
 *
 * Returns the client encoding as a String.
 */
VALUE
pgconn_client_encoding( obj)
    VALUE obj;
{
    char *encoding = (char *) pg_encoding_to_char(
                                PQclientEncoding( get_pgconn( obj)));
    return rb_tainted_str_new2( encoding);
}

/*
 * call-seq:
 *    conn.set_client_encoding( encoding )
 *
 * Sets the client encoding to the _encoding_ String.
 */
VALUE
pgconn_set_client_encoding( obj, str)
    VALUE obj, str;
{
    Check_Type( str, T_STRING);
    if ((PQsetClientEncoding( get_pgconn( obj), STR2CSTR( str))) == -1) {
        rb_raise( rb_ePGError, "invalid encoding name %s", str);
    }
    return Qnil;
}

#define SCALE_MASK 0xffff

int
has_numeric_scale( typmod)
    int typmod;
{
    if (typmod == -1) return 1;
    return (typmod - VARHDRSZ) & SCALE_MASK;
}

#define PARSE( klass, string) \
    rb_funcall( klass, rb_intern( "parse"), 1, rb_tainted_str_new2( string));

VALUE
fetch_pgresult( result, row, column)
    PGresult *result;
    int row;
    int column;
{
    char* string;

    if (PQgetisnull( result, row, column))
        return Qnil;

    string = PQgetvalue( result, row, column);

    if (!translate_results)
        return rb_tainted_str_new2( string);

    switch (PQftype( result, column)) {
    case BOOLOID:
        return *string == 't' ? Qtrue : Qfalse;

    case BYTEAOID:
        return pgconn_s_unescape_bytea( rb_cPGConn, rb_tainted_str_new2( string));

    case NUMERICOID:
        if (has_numeric_scale( PQfmod( result, column))) {
            return rb_funcall( rb_cBigDecimal, id_new,
                        1, rb_tainted_str_new2( string));
        }
        /* when scale == 0 return inum */

    case INT8OID:
    case INT4OID:
    case INT2OID:
    case OIDOID:
        return rb_cstr2inum( string, 10);

    case FLOAT8OID:
    case FLOAT4OID:
        return rb_float_new( rb_cstr_to_dbl( string, Qfalse));

    case DATEOID:
        return PARSE( rb_cDate, string);
    case TIMEOID:
    case TIMETZOID:
        return PARSE( rb_cTime, string);
    case TIMESTAMPOID:
    case TIMESTAMPTZOID:
        return PARSE( rb_cDateTime, string);

    default:
        return rb_tainted_str_new2( string);
    }
}


VALUE
fetch_pgrow( result, row_num)
    PGresult *result;
    int row_num;
{
    VALUE row;
    VALUE fields;
    int i;

    fields = fetch_fields( result);
    row = rb_funcall( rb_cPGRow, id_new, 1, fields);
    for (i = 0; i < RARRAY( fields)->len; i++) {
        /* don't use push, Pg::Row is sized with nils in #new */
        rb_ary_store( row, i, fetch_pgresult( result, row_num, i));
    }
    return row;
}

/*
 * call-seq:
 *   conn.select_one( query, *bind_values)
 *
 * Return the first row of the query results.
 * Equivalent to conn.query( query, *bind_values).first
 */
VALUE
pgconn_select_one( argc, argv, self)
    int argc;
    VALUE *argv;
    VALUE self;
{
    VALUE res;
    VALUE row;
    PGresult *result;


    res = rb_funcall2( self, rb_intern( "exec"), argc, argv);
    result = get_pgresult( res);
    if (PQntuples( result))
      row = fetch_pgrow( result, 0);
    else
      row = Qnil;
    pgresult_clear( res);
    return row;
}

/*
 * call-seq:
 *   conn.select_value( query, *bind_values)
 *
 * Return the first value of the first row of the query results.
 * Equivalent to conn.query( query, *bind_values).first.first
 */
VALUE
pgconn_select_value( argc, argv, self)
    int argc;
    VALUE *argv;
    VALUE self;
{
    VALUE res;
    VALUE value;
    PGresult *result;

    res = rb_funcall2( self, rb_intern( "exec"), argc, argv);
    result = get_pgresult( res);
    if (PQntuples( result))
      value = fetch_pgresult( result, 0, 0);
    else
      value = Qnil;
    pgresult_clear( res);
    return value;
}

/*
 * call-seq:
 *   conn.select_values( query, *bind_values)
 *
 * Equivalent to conn.query( query, *bind_values).flatten
 */
VALUE
pgconn_select_values( argc, argv, self)
    int argc;
    VALUE *argv;
    VALUE self;
{
    VALUE pg_result = rb_funcall2( self, rb_intern( "exec"), argc, argv);
    PGresult *result = get_pgresult( pg_result);
    int ntuples = PQntuples( result);
    int nfields = PQnfields( result);

    VALUE values = rb_ary_new2( ntuples * nfields);
    int row_num, field_num;
    for (row_num = 0; row_num < ntuples; row_num++) {
      for (field_num = 0; field_num < nfields; field_num++) {
        rb_ary_push( values, fetch_pgresult( result, row_num, field_num));
      }
    }

    pgresult_clear( pg_result);
    return values;
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


void
Init_pgsql( void)
{
    rb_require( "bigdecimal");
    rb_require( "date");
    rb_require( "time");
    rb_cBigDecimal = RUBY_CLASS( "BigDecimal");
    rb_cRational   = RUBY_CLASS( "Rational");
    rb_cDate       = RUBY_CLASS( "Date");
    rb_cDateTime   = RUBY_CLASS( "DateTime");

    init_pg_module();
    init_pg_large();
    init_pg_row();
    init_pg_result();

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
    rb_define_method( rb_cPGConn, "db", pgconn_db, 0);
    rb_define_alias( rb_cPGConn, "dbname", "db");
    rb_define_method( rb_cPGConn, "host", pgconn_host, 0);
    rb_define_method( rb_cPGConn, "options", pgconn_options, 0);
    rb_define_method( rb_cPGConn, "port", pgconn_port, 0);
    rb_define_method( rb_cPGConn, "tty", pgconn_tty, 0);
    rb_define_method( rb_cPGConn, "status", pgconn_status, 0);
    rb_define_method( rb_cPGConn, "error", pgconn_error, 0);
    rb_define_method( rb_cPGConn, "close", pgconn_close, 0);
    rb_define_alias( rb_cPGConn, "finish", "close");
    rb_define_method( rb_cPGConn, "reset", pgconn_reset, 0);
    rb_define_method( rb_cPGConn, "user", pgconn_user, 0);
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
    rb_define_method( rb_cPGConn, "protocol_version",
                                                   pgconn_protocol_version, 0);
    rb_define_method( rb_cPGConn, "server_version", pgconn_server_version, 0);
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

    id_new        = rb_intern( "new");
    id_on_notice  = rb_intern( "@on_notice");
    id_gsub       = rb_intern( "gsub");
    id_gsub_bang  = rb_intern( "gsub!");

    pg_escape_regex = rb_reg_new( "([\\t\\n\\\\])", 10, 0);
    rb_global_variable( &pg_escape_regex);
    pg_escape_str = rb_str_new( "\\\\\\1", 4);
    rb_global_variable( &pg_escape_str);
}

