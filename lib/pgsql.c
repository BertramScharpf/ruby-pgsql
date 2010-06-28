/*
 *  pgsql.c  --  Pg::Xxx classes
 */

#include "pgsql.h"

#include <st.h>


static VALUE rb_cBigDecimal;
static VALUE rb_cRational;
static VALUE rb_cDate;
static VALUE rb_cDateTime;

static VALUE rb_ePGExecError;
static VALUE rb_ePGConnError;
static VALUE rb_ePGResError;
static VALUE rb_cPGConn;
static VALUE rb_cPGResult;
static VALUE rb_cPGLarge;
static VALUE rb_cPGRow;

static ID id_new;
static ID id_savepoints;
static ID id_on_notice;
static ID id_keys;
static ID id_gsub;
static ID id_gsub_bang;

static VALUE pg_escape_regex;
static VALUE pg_escape_str;

static int translate_results = 1;

static VALUE pgreserror_new( PGresult *ptr);
static VALUE pgreserror_status( VALUE obj);
static VALUE pgreserror_sqlst( VALUE self);
static VALUE pgreserror_primary( VALUE self);
static VALUE pgreserror_detail( VALUE self);
static VALUE pgreserror_hint( VALUE self);

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
static PGresult *get_pgresult( VALUE obj);
static VALUE yield_or_return_result( VALUE res);

static VALUE pgconn_exec( int argc, VALUE *argv, VALUE obj);
static VALUE pgconn_async_exec( VALUE obj, VALUE str);
static VALUE pgconn_query( int argc, VALUE *argv, VALUE obj);
static VALUE pgconn_async_query( VALUE obj, VALUE str);
static VALUE pgconn_get_notify( VALUE obj);

static void free_pgconn( PGconn *ptr);
static VALUE pgconn_insert_table( VALUE obj, VALUE table, VALUE values);
static VALUE rescue_transaction( VALUE obj);
static VALUE ensure_transaction( VALUE obj);
static VALUE yield_transaction( VALUE obj);
static VALUE pgconn_transaction( int argc, VALUE *argv, VALUE obj);

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

static void free_pgresult( PGresult *ptr);
static int has_numeric_scale( int typmod);
static VALUE fetch_pgresult( PGresult *result, int row, int column);

static VALUE fetch_pgrow( PGresult *result, int row_num);
static VALUE pgconn_select_one( int argc, VALUE *argv, VALUE self);
static VALUE pgconn_select_value( int argc, VALUE *argv, VALUE self);
static VALUE pgconn_select_values( int argc, VALUE *argv, VALUE self);

static VALUE pgresult_new( PGconn *conn, PGresult *ptr);
static VALUE pgresult_status( VALUE obj);

static VALUE pgresult_each( VALUE self);
static VALUE pgresult_aref( int argc, VALUE *argv, VALUE obj);
static VALUE fetch_fields( PGresult *result);
static VALUE pgresult_fields( VALUE obj);
static VALUE pgresult_num_tuples( VALUE obj);
static VALUE pgresult_num_fields( VALUE obj);
static VALUE pgresult_fieldname( VALUE obj, VALUE index);
static VALUE pgresult_fieldnum( VALUE obj, VALUE name);
static VALUE pgresult_type( VALUE obj, VALUE index);
static VALUE pgresult_size( VALUE obj, VALUE index);
static VALUE pgresult_getvalue( VALUE obj, VALUE tup_num, VALUE field_num);

static VALUE pgresult_getvalue_byname( VALUE obj, VALUE tup_num,
                                                          VALUE field_name);

static VALUE pgresult_getlength( VALUE obj, VALUE tup_num, VALUE field_num);
static VALUE pgresult_getisnull( VALUE obj, VALUE tup_num, VALUE field_num);
static VALUE pgresult_print( VALUE obj, VALUE file, VALUE opt);
static VALUE pgresult_cmdtuples( VALUE obj);
static VALUE pgresult_cmdstatus( VALUE obj);
static VALUE pgresult_oid( VALUE obj);
static VALUE pgresult_clear( VALUE obj);
static VALUE pgresult_result_with_clear( VALUE self);

static PGlarge *get_pglarge( VALUE obj);
static VALUE pgconn_loimport( VALUE obj, VALUE filename);
static VALUE pgconn_loexport( VALUE obj, VALUE lo_oid, VALUE filename);
static VALUE pglarge_close( VALUE obj);

static VALUE pgconn_locreate( int argc, VALUE *argv, VALUE obj);
static VALUE pgconn_loopen( int argc, VALUE *argv, VALUE obj);
static VALUE pgconn_lounlink( VALUE obj, VALUE lo_oid);
static void free_pglarge( PGlarge *ptr);
static VALUE pglarge_new( PGconn *conn, Oid lo_oid, int lo_fd);
static VALUE pglarge_oid( VALUE obj);

static int   large_tell(  PGlarge *pglarge);
static int   large_lseek( PGlarge *pglarge, int offset, int whence);
static VALUE pglarge_tell( VALUE obj);
static VALUE loread_all(   VALUE obj);
static VALUE pglarge_read( int argc, VALUE *argv, VALUE obj);
static VALUE pglarge_write( VALUE obj, VALUE buffer);
static VALUE pglarge_seek( VALUE obj, VALUE offset, VALUE whence);
static VALUE pglarge_size( VALUE obj);
static VALUE pglarge_export( VALUE obj, VALUE filename);
static VALUE pglarge_unlink( VALUE obj);
static VALUE pgrow_init( VALUE self, VALUE keys);
static VALUE pgrow_keys( VALUE self);
static VALUE pgrow_values( VALUE self);
static VALUE pgrow_aref( int argc, VALUE * argv, VALUE self);
static VALUE pgrow_each_value( VALUE self);
static VALUE pgrow_each_pair( VALUE self);
static VALUE pgrow_each( VALUE self);
static VALUE pgrow_each_key( VALUE self);
static VALUE pgrow_to_hash( VALUE self);



static PGresult *pg_pqexec( PGconn *conn, const char *cmd);
static void      pg_raise_exec( PGconn *conn);



void pg_raise_exec( PGconn * conn)
{
    rb_raise( rb_ePGExecError, PQerrorMessage( conn));
}

PGresult *pg_pqexec( PGconn * conn, const char *cmd)
{
    PGresult *result;

    result = PQexec( conn, cmd);
    if (result == NULL) {
        pg_raise_exec( conn);
    }
    return result;
}



static VALUE
pgreserror_new( result)
    PGresult *result;
{
    VALUE res, argv[ 1];
    res = Data_Wrap_Struct( rb_ePGResError, 0, free_pgresult, result);
    argv[ 0] = rb_str_new2( PQresultErrorMessage( result));
    rb_obj_call_init( res, 1, argv);
    return res;
}

static VALUE
pgreserror_status( self)
    VALUE self;
{
    return INT2NUM( PQresultStatus( get_pgresult( self)));
}

/*
 * call-seq:
 *   pgqe.sqlstate() => string
 *
 * Forward PostgreSQL's error code.
 *
 */
static VALUE
pgreserror_sqlst( self)
    VALUE self;
{
    char *e;

    e = PQresultErrorField( get_pgresult( self), PG_DIAG_SQLSTATE);
    return rb_str_new2( e);
}


/*
 * call-seq:
 *   pgqe.primary() => string
 *
 * Forward PostgreSQL's error details.
 *
 */
static VALUE
pgreserror_primary( self)
    VALUE self;
{
    char *e;

    e = PQresultErrorField( get_pgresult( self), PG_DIAG_MESSAGE_PRIMARY);
    return rb_str_new2( e);
}


/*
 * call-seq:
 *   pgqe.details() => string
 *
 * Forward PostgreSQL's error details.
 *
 */
static VALUE
pgreserror_detail( self)
    VALUE self;
{
    char *e;

    e = PQresultErrorField( get_pgresult( self), PG_DIAG_MESSAGE_DETAIL);
    return e == NULL ? Qnil : rb_str_new2( e);
}


/*
 * call-seq:
 *   pgqe.hint() => string
 *
 * Forward PostgreSQL's error hint.
 *
 */
static VALUE
pgreserror_hint( self)
    VALUE self;
{
    char *e;

    e = PQresultErrorField( get_pgresult( self), PG_DIAG_MESSAGE_HINT);
    return e == NULL ? Qnil : rb_str_new2( e);
}



static VALUE
pgconn_alloc( klass)
    VALUE klass;
{
    return Data_Wrap_Struct( klass, 0, free_pgconn, NULL);
}


static PGconn *
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

static PGconn *
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
static VALUE
pgconn_s_translate_results_set( self, fact)
    VALUE self, fact;
{
    translate_results = RTEST( fact) ? 1 : 0;
    return Qnil;
}

static VALUE
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

static VALUE
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

static VALUE
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
static VALUE
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

static int
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
static VALUE
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
static VALUE
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
static VALUE
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
static VALUE
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

    if (PQstatus( conn) == CONNECTION_BAD) {
        rb_raise( rb_ePGConnError, PQerrorMessage( conn));
    }

    Data_Set_Struct( self, conn);
    return self;
}

static PGconn*
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
static VALUE
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
static VALUE
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
static VALUE
pgconn_reset( obj)
    VALUE obj;
{
    PQreset( get_pgconn( obj));
    return obj;
}

static PGresult*
get_pgresult( obj)
    VALUE obj;
{
    PGresult *result;
    Data_Get_Struct( obj, PGresult, result);
    if (result == NULL)
        rb_raise( rb_ePGError, "query not performed");
    return result;
}

static VALUE
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
static VALUE
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
        if (result == NULL)
            pg_raise_exec( conn);
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
static VALUE
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
static VALUE
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
static VALUE
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
static VALUE
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

static void
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
static VALUE
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


static VALUE
rescue_transaction( obj)
    VALUE obj;
{
    PGconn *conn;
    VALUE s;
    char b_rol[ 48];

    conn = get_pgconn( obj);
    s = rb_ivar_get( obj, id_savepoints);

    if (RARRAY( s)->len - 1 <= 0) {
        sprintf( b_rol, "rollback");
    } else {
        sprintf( b_rol, "rollback to savepoint %s",
                                        STR2CSTR( rb_ary_entry( s, 0)));
    }
    pg_pqexec( conn, b_rol);
    rb_exc_raise( ruby_errinfo);
    return Qnil;
}

static VALUE
ensure_transaction( obj)
    VALUE obj;
{
    VALUE s;

    s = rb_ivar_get( obj, id_savepoints);
    rb_ary_shift( s);
    return Qnil;
}

static VALUE
yield_transaction( obj)
    VALUE obj;
{
    return rb_rescue( rb_yield, obj, rescue_transaction, obj);
}

/*
 * call-seq:
 *    conn.transaction { |conn| ... }
 *
 * Open and close a transaction block. The blocks may be nested.
 */
static VALUE
pgconn_transaction( argc, argv, self)
    int argc;
    VALUE *argv;
    VALUE self;
{
    PGconn *conn;
    VALUE result;
    VALUE s, t;
    char b_beg[ 32], b_com[ 32];

    conn = get_pgconn( self);

    s = rb_ivar_get( (VALUE) self, id_savepoints);
    if (NIL_P( s)) {
        s = rb_ary_new();
        rb_ivar_set( self, id_savepoints, s);
    }

    if (RARRAY( s)->len == 0) {
        t = Qnil;
        rb_ary_unshift( s, rb_str_new( "pgsql_0000", 10));
        sprintf( b_beg, "begin");
        sprintf( b_com, "commit");
    } else {
        t = rb_funcall( rb_ary_entry( s, 0), rb_intern( "succ"), 0);
        rb_ary_unshift( s, t);
        sprintf( b_beg, "savepoint %s", STR2CSTR( t));
        sprintf( b_com, "release savepoint %s", STR2CSTR( t));
    }
    pg_pqexec( conn, b_beg);
    result = rb_ensure( yield_transaction, self, ensure_transaction, self);
    pg_pqexec( conn, b_com);
    return result;
}


/*
 * call-seq:
 *    conn.putline()
 *
 * Sends the string to the backend server.
 * Users must send a single "." to denote the end of data transmission.
 */
static VALUE
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
static VALUE
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
static VALUE
pgconn_endcopy( obj)
    VALUE obj;
{
    if (PQendcopy( get_pgconn( obj)) == 1) {
        rb_raise( rb_ePGError, "cannot complete copying");
    }
    return Qnil;
}

static void
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
static VALUE
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
static VALUE
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
static VALUE
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
static VALUE
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
static VALUE
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
static VALUE
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
static VALUE
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
static VALUE
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
static VALUE
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
static VALUE
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
static VALUE
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
static VALUE
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
static VALUE
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
static VALUE
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
static VALUE
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
static VALUE
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
static VALUE
pgconn_set_client_encoding( obj, str)
    VALUE obj, str;
{
    Check_Type( str, T_STRING);
    if ((PQsetClientEncoding( get_pgconn( obj), STR2CSTR( str))) == -1) {
        rb_raise( rb_ePGError, "invalid encoding name %s", str);
    }
    return Qnil;
}

static void
free_pgresult( ptr)
    PGresult *ptr;
{
    PQclear( ptr);
}

#define SCALE_MASK 0xffff

static int
has_numeric_scale( typmod)
    int typmod;
{
    if (typmod == -1) return 1;
    return (typmod - VARHDRSZ) & SCALE_MASK;
}

#define PARSE( klass, string) \
    rb_funcall( klass, rb_intern( "parse"), 1, rb_tainted_str_new2( string));

static VALUE
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


static VALUE
pgresult_new( conn, result)
    PGconn   *conn;
    PGresult *result;
{
    VALUE res;

    switch (PQresultStatus( result)) {
    case PGRES_TUPLES_OK:
    case PGRES_COPY_OUT:
    case PGRES_COPY_IN:
    case PGRES_EMPTY_QUERY:
    case PGRES_COMMAND_OK:      /* no data will be received */
        break;

    case PGRES_BAD_RESPONSE:
    case PGRES_FATAL_ERROR:
    case PGRES_NONFATAL_ERROR:
        rb_exc_raise( pgreserror_new( result));
        break;
    default:
        PQclear( result);
        rb_raise( rb_ePGError, "internal error: unknown result status.");
        break;
    }

    res = Data_Wrap_Struct( rb_cPGResult, 0, free_pgresult, result);
    rb_obj_call_init( res, 0, NULL);
    return res;
}


/*
 * call-seq:
 *    res.status()
 *
 * Returns the status of the query. The status value is one of:
 * * +EMPTY_QUERY+
 * * +COMMAND_OK+
 * * +TUPLES_OK+
 * * +COPY_OUT+
 * * +COPY_IN+
 */
static VALUE
pgresult_status( obj)
    VALUE obj;
{
    return INT2NUM( PQresultStatus( get_pgresult( obj)));
}


static VALUE
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
static VALUE
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
static VALUE
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
static VALUE
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
 *    res.each{ |tuple| ... }  ->  nil or int
 *
 * Invokes the block for each tuple (row) in the result.
 *
 * Return the number of rows the query resulted in, or +nil+ if there
 * wasn't any (like <code>Numeric#nonzero?</code>).
 */
static VALUE
pgresult_each( self)
    VALUE self;
{
    PGresult *result = get_pgresult( self);
    int row_count = PQntuples( result);
    int row_num, r;

    for (row_num = 0, r = row_count; r ; row_num++, r--)
        rb_yield( fetch_pgrow( result, row_num));

    return row_count ? INT2NUM( row_count) : Qnil;
}

/*
 * call-seq:
 *    res[ n ]
 *
 * Returns the tuple (row) corresponding to _n_. Returns +nil+ if <tt>_n_ >=
 * res.num_tuples</tt>.
 *
 * Equivalent to <tt>res.result[n]</tt>.
 */
static VALUE
pgresult_aref( argc, argv, obj)
    int argc;
    VALUE *argv;
    VALUE obj;
{
    PGresult *result;
    VALUE a1, a2, val;
    int i, j, nf, nt;

    result = get_pgresult( obj);
    nt = PQntuples( result);
    nf = PQnfields( result);
    switch (rb_scan_args( argc, argv, "11", &a1, &a2)) {
    case 1:
        i = NUM2INT( a1);
        if( i >= nt ) return Qnil;

        val = rb_ary_new();
        for (j=0; j<nf; j++) {
            VALUE value = fetch_pgresult( result, i, j);
            rb_ary_push( val, value);
        }
        return val;

    case 2:
        i = NUM2INT( a1);
        if( i >= nt ) return Qnil;
        j = NUM2INT( a2);
        if( j >= nf ) return Qnil;
        return fetch_pgresult( result, i, j);

    default:
        return Qnil;            /* not reached */
    }
}

static VALUE
fetch_fields( result)
    PGresult *result;
{
    VALUE ary;
    int n, i;

    n = PQnfields( result);
    ary = rb_ary_new2( n);
    for (i=0;i<n;i++) {
        rb_ary_push( ary, rb_tainted_str_new2( PQfname( result, i)));
    }
    return ary;
}

/*
 * call-seq:
 *    res.fields()
 *
 * Returns an array of Strings representing the names of the fields in the
 * result.
 *
 *   res=conn.exec( "SELECT foo, bar AS biggles, jim, jam FROM mytable")
 *   res.fields => [ 'foo' , 'biggles' , 'jim' , 'jam' ]
 */
static VALUE
pgresult_fields( obj)
    VALUE obj;
{
    return fetch_fields( get_pgresult( obj));
}

/*
 * call-seq:
 *    res.num_tuples()
 *
 * Returns the number of tuples (rows) in the query result.
 *
 * Similar to <tt>res.result.length</tt> (but faster).
 */
static VALUE
pgresult_num_tuples( obj)
    VALUE obj;
{
    int n;

    n = PQntuples( get_pgresult( obj));
    return INT2NUM( n);
}

/*
 * call-seq:
 *    res.num_fields()
 *
 * Returns the number of fields (columns) in the query result.
 *
 * Similar to <tt>res.result[0].length</tt> (but faster).
 */
static VALUE
pgresult_num_fields( obj)
    VALUE obj;
{
    int n;

    n = PQnfields( get_pgresult( obj));
    return INT2NUM( n);
}

/*
 * call-seq:
 *    res.fieldname( index )
 *
 * Returns the name of the field (column) corresponding to the index.
 *
 *   res=conn.exec( "SELECT foo, bar AS biggles, jim, jam FROM mytable")
 *   puts res.fieldname( 2) => 'jim'
 *   puts res.fieldname( 1) => 'biggles'
 *
 * Equivalent to <tt>res.fields[_index_]</tt>.
 */
static VALUE
pgresult_fieldname( obj, index)
    VALUE obj, index;
{
    PGresult *result;
    int i = NUM2INT( index);
    char *name;

    result = get_pgresult( obj);
    if (i < 0 || i >= PQnfields( result)) {
        rb_raise( rb_eArgError, "invalid field number %d", i);
    }
    name = PQfname( result, i);
    return rb_tainted_str_new2( name);
}

/*
 * call-seq:
 *    res.fieldnum( name )
 *
 * Returns the index of the field specified by the string _name_.
 *
 *   res=conn.exec( "SELECT foo, bar AS biggles, jim, jam FROM mytable")
 *   puts res.fieldnum('foo') => 0
 *
 * Raises an ArgumentError if the specified _name_ isn't one of the field
 * names; raises a TypeError if _name_ is not a String.
 */
static VALUE
pgresult_fieldnum( obj, name)
    VALUE obj, name;
{
    int n;

    Check_Type( name, T_STRING);

    n = PQfnumber( get_pgresult( obj), STR2CSTR( name));
    if (n == -1) {
        rb_raise( rb_eArgError, "Unknown field: %s", STR2CSTR( name));
    }
    return INT2NUM( n);
}

/*
 * call-seq:
 *    res.type( index )
 *
 * Returns the data type associated with the given column number.
 *
 * The integer returned is the internal +OID+ number (in PostgreSQL) of the
 * type. If you have the PostgreSQL source available, you can see the OIDs for
 * every column type in the file <tt>src/include/catalog/pg_type.h</tt>.
 */
static VALUE
pgresult_type( obj, index)
    VALUE obj, index;
{
    PGresult* result = get_pgresult( obj);
    int i = NUM2INT( index);
    if (i < 0 || i >= PQnfields( result)) {
        rb_raise( rb_eArgError, "invalid field number %d", i);
    }
    return INT2NUM( PQftype( result, i));
}

/*
 * call-seq:
 *    res.size( index )
 *
 * Returns the size of the field type in bytes.  Returns <tt>-1</tt> if the
 * field is variable sized.
 *
 *   res = conn.exec( "SELECT myInt, myVarChar50 FROM foo")
 *   res.size( 0) => 4
 *   res.size( 1) => -1
 */
static VALUE
pgresult_size( obj, index)
    VALUE obj, index;
{
    PGresult *result;
    int i = NUM2INT( index);
    int size;

    result = get_pgresult( obj);
    if (i < 0 || i >= PQnfields( result)) {
        rb_raise( rb_eArgError, "invalid field number %d", i);
    }
    size = PQfsize( result, i);
    return INT2NUM( size);
}

/*
 * call-seq:
 *    res.value( tup_num, field_num )
 *
 * Returns the value in tuple number <i>tup_num</i>, field number
 * <i>field_num</i>. (Row <i>tup_num</i>, column <i>field_num</i>.)
 *
 * Equivalent to <tt>res.result[<i>tup_num</i>][<i>field_num</i>]</tt> (but
 * faster).
 */
static VALUE
pgresult_getvalue( obj, tup_num, field_num)
    VALUE obj, tup_num, field_num;
{
    PGresult *result;
    int i = NUM2INT( tup_num);
    int j = NUM2INT( field_num);

    result = get_pgresult( obj);
    if (i < 0 || i >= PQntuples( result)) {
        rb_raise( rb_eArgError, "invalid tuple number %d", i);
    }
    if (j < 0 || j >= PQnfields( result)) {
        rb_raise( rb_eArgError, "invalid field number %d", j);
    }

    return fetch_pgresult( result, i, j);
}


/*
 * call-seq:
 *    res.value_byname( tup_num, field_name )
 *
 * Returns the value in tuple number <i>tup_num</i>, for the field named
 * <i>field_name</i>.
 *
 * Equivalent to (but faster than) either of:
 *    res.result[<i>tup_num</i>][ res.fieldnum(<i>field_name</i>) ]
 *    res.value( <i>tup_num</i>, res.fieldnum(<i>field_name</i>) )
 *
 * <i>(This method internally calls #value as like the second example above;
 * it is slower than using the field index directly.)</i>
 */
static VALUE
pgresult_getvalue_byname( obj, tup_num, field_name)
    VALUE obj, tup_num, field_name;
{
    return pgresult_getvalue( obj, tup_num,
                pgresult_fieldnum( obj, field_name));
}


/*
 * call-seq:
 *    res.getlength( tup_num, field_num )
 *
 * Returns the (String) length of the field in bytes.
 *
 * Equivalent to <tt>res.value(<i>tup_num</i>,<i>field_num</i>).length</tt>.
 */
static VALUE
pgresult_getlength( obj, tup_num, field_num)
    VALUE obj, tup_num, field_num;
{
    PGresult *result;
    int i = NUM2INT( tup_num);
    int j = NUM2INT( field_num);

    result = get_pgresult( obj);
    if (i < 0 || i >= PQntuples( result)) {
        rb_raise( rb_eArgError, "invalid tuple number %d", i);
    }
    if (j < 0 || j >= PQnfields( result)) {
        rb_raise( rb_eArgError, "invalid field number %d", j);
    }
    return INT2FIX( PQgetlength( result, i, j));
}

/*
 * call-seq:
 *    res.getisnull( tuple_position, field_position) -> boolean
 *
 * Returns +true+ if the specified value is +nil+; +false+ otherwise.
 *
 * Equivalent to <tt>res.value(<i>tup_num</i>,<i>field_num</i>)==+nil+</tt>.
 */
static VALUE
pgresult_getisnull( obj, tup_num, field_num)
    VALUE obj, tup_num, field_num;
{
    PGresult *result;
    int i = NUM2INT( tup_num);
    int j = NUM2INT( field_num);

    result = get_pgresult( obj);
    if (i < 0 || i >= PQntuples( result)) {
        rb_raise( rb_eArgError, "invalid tuple number %d", i);
    }
    if (j < 0 || j >= PQnfields( result)) {
        rb_raise( rb_eArgError, "invalid field number %d", j);
    }
    return PQgetisnull( result, i, j) ? Qtrue : Qfalse;
}

/*
 * call-seq:
 *    res.print( file, opt )
 *
 * MISSING: Documentation
 */
static VALUE
pgresult_print( obj, file, opt)
    VALUE obj, file, opt;
{
    VALUE value;
    ID mem;
    OpenFile* fp;
    PQprintOpt po;

    Check_Type( file, T_FILE);
    Check_Type( opt,  T_STRUCT);
    GetOpenFile( file, fp);

    memset(&po, 0, sizeof (po));

    mem = rb_intern( "header");
    value = rb_struct_getmember( opt, mem);
    po.header = value == Qtrue ? 1 : 0;

    mem = rb_intern( "align");
    value = rb_struct_getmember( opt, mem);
    po.align = value == Qtrue ? 1 : 0;

    mem = rb_intern( "standard");
    value = rb_struct_getmember( opt, mem);
    po.standard = value == Qtrue ? 1 : 0;

    mem = rb_intern( "html3");
    value = rb_struct_getmember( opt, mem);
    po.html3 = value == Qtrue ? 1 : 0;

    mem = rb_intern( "expanded");
    value = rb_struct_getmember( opt, mem);
    po.expanded = value == Qtrue ? 1 : 0;

    mem = rb_intern( "pager");
    value = rb_struct_getmember( opt, mem);
    po.pager = value == Qtrue ? 1 : 0;

    mem = rb_intern( "fieldSep");
    value = rb_struct_getmember( opt, mem);
    if (!NIL_P( value)) {
        Check_Type( value, T_STRING);
        po.fieldSep = STR2CSTR( value);
    }

    mem = rb_intern( "tableOpt");
    value = rb_struct_getmember( opt, mem);
    if (!NIL_P( value)) {
        Check_Type( value, T_STRING);
        po.tableOpt = STR2CSTR( value);
    }

    mem = rb_intern( "caption");
    value = rb_struct_getmember( opt, mem);
    if (!NIL_P( value)) {
        Check_Type( value, T_STRING);
        po.caption = STR2CSTR( value);
    }

    PQprint( fp->f2?fp->f2:fp->f, get_pgresult( obj), &po);
    return obj;
}

/*
 * call-seq:
 *    res.cmdtuples()
 *
 * Returns the number of tuples (rows) affected by the SQL command.
 *
 * If the SQL command that generated the Pg::Result was not one of +INSERT+,
 * +UPDATE+, +DELETE+, +MOVE+, or +FETCH+, or if no tuples (rows) were
 * affected, <tt>0</tt> is returned.
 */
static VALUE
pgresult_cmdtuples( obj)
    VALUE obj;
{
    long n;

    n = strtol( PQcmdTuples( get_pgresult( obj)), NULL, 10);
    return INT2NUM( n);
}
/*
 * call-seq:
 *    res.cmdstatus()
 *
 * Returns the status string of the last query command.
 */
static VALUE
pgresult_cmdstatus( obj)
    VALUE obj;
{
    return rb_tainted_str_new2( PQcmdStatus( get_pgresult( obj)));
}

/*
 * call-seq:
 *    res.oid()
 *
 * Returns the +oid+.
 */
static VALUE
pgresult_oid( obj)
    VALUE obj;
{
    Oid n = PQoidValue( get_pgresult( obj));
    return  n == InvalidOid ? Qnil : INT2NUM( n);
}

/*
 * call-seq:
 *    res.clear()
 *
 * Clears the Pg::Result object as the result of the query.
 */
static VALUE
pgresult_clear( obj)
    VALUE obj;
{
    if (DATA_PTR( obj) != NULL) {
      PQclear( get_pgresult( obj));
      DATA_PTR( obj) = NULL;
    }
    return Qnil;
}

static VALUE
pgresult_result_with_clear( self)
    VALUE self;
{
    if (rb_block_given_p()) {
        return rb_ensure( pgresult_each, self, pgresult_clear, self);
    }
    else {
        VALUE rows = rb_funcall( self, rb_intern( "rows"), 0);
        pgresult_clear( self);
        return rows;
    }
}


/* Large Object support */
static PGlarge*
get_pglarge( obj)
    VALUE obj;
{
    PGlarge *pglarge;

    Data_Get_Struct( obj, PGlarge, pglarge);
    if (pglarge == NULL)
      rb_raise( rb_ePGError, "invalid large object");
    return pglarge;
}

/*
 * call-seq:
 *    conn.lo_import( file) -> Pg::Large
 *
 * Import a file to a large object. Returns a Pg::Large instance on success. On
 * failure, it raises a PGError exception.
 */
static VALUE
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
    return pglarge_new( conn, lo_oid, -1);
}

/*
 * call-seq:
 *    conn.lo_export( oid, file )
 *
 * Saves a large object of _oid_ to a _file_.
 */
static VALUE
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
 *    lrg.close()
 *
 * Closes a large object. Closed when they are garbage-collected.
 */
static VALUE
pglarge_close( obj)
    VALUE obj;
{
    PGlarge *pglarge;
    int ret;

    pglarge = get_pglarge( obj);
    ret = lo_close( pglarge->pgconn, pglarge->lo_fd);
    if (ret < 0)
        rb_raise( rb_ePGError, "cannot close large object");
    DATA_PTR( obj) = 0;
    return Qnil;
}


/*
 * call-seq:
 *    conn.lo_create( [mode] ) -> Pg::Large
 *    conn.lo_create( [mode] ) { |pglarge| ... } -> obj
 *
 * Returns a Pg::Large instance on success. On failure, it raises PGError
 * exception. <i>(See #lo_open for information on _mode_.)</i>
 *
 * If a block is given, the blocks result is returned.
 *
 */
static VALUE
pgconn_locreate( argc, argv, obj)
    int argc;
    VALUE *argv;
    VALUE obj;
{
    VALUE nmode;
    int mode;
    PGconn *conn;
    Oid lo_oid;
    int fd;
    VALUE lob;

    if (rb_scan_args( argc, argv, "01", &nmode) == 0)
        mode = INV_WRITE;
    else
        mode = FIX2INT( nmode);

    conn = get_pgconn( obj);
    lo_oid = lo_creat( conn, mode);
    if (lo_oid == 0)
        rb_raise( rb_ePGError, "can't creat large object");
    fd = lo_open( conn, lo_oid, mode);
    if (fd < 0)
        rb_raise( rb_ePGError, "can't open large object");

    lob = pglarge_new( conn, lo_oid, fd);
    if (rb_block_given_p())
        return rb_ensure( rb_yield, lob, pglarge_close, lob);
    else
        return lob;
}

/*
 * call-seq:
 *    conn.lo_open( oid, [mode] ) -> Pg::Large
 *    conn.lo_open( oid, [mode] ) { |pglarge| ... } -> obj
 *
 * Open a large object of _oid_. Returns a Pg::Large instance on success.
 * The _mode_ argument specifies the mode for the opened large object,
 * which is either +INV_READ+, or +INV_WRITE+.
 * * If _mode_ On failure, it raises a PGError exception.
 * * If _mode_ is omitted, the default is +INV_READ+.
 *
 * If a block is given, the blocks result is returned.
 *
 */
static VALUE
pgconn_loopen( argc, argv, obj)
    int argc;
    VALUE *argv;
    VALUE obj;
{
    VALUE nmode, objid;
    Oid lo_oid;
    int mode;
    int fd;
    PGconn *conn;
    VALUE lob;

    switch (rb_scan_args( argc, argv, "11", &objid, &nmode)) {
    case 1:
        lo_oid = NUM2INT( objid);
        mode = INV_READ;
        break;
    case 2:
        lo_oid = NUM2INT( objid);
        mode = FIX2INT( nmode);
        break;
    default:
        return Qnil;            /* not reached */
    }
    conn = get_pgconn( obj);
    fd = lo_open( conn, lo_oid, mode);
    if (fd < 0)
        rb_raise( rb_ePGError, "can't open large object");

    lob = pglarge_new( conn, lo_oid, fd);
    if (rb_block_given_p())
        return rb_ensure( rb_yield, lob, pglarge_close, lob);
    else
        return lob;
}

/*
 * call-seq:
 *    conn.lo_unlink( oid )
 *
 * Unlinks (deletes) the postgres large object of _oid_.
 */
static VALUE
pgconn_lounlink( obj, lo_oid)
    VALUE obj, lo_oid;
{
    PGconn *conn;
    int oid;
    int result;

    oid = NUM2INT( lo_oid);
    if (oid < 0)
        rb_raise( rb_ePGError, "invalid oid %d", oid);
    conn = get_pgconn( obj);
    result = lo_unlink( conn, oid);
    if (result < 0)
        rb_raise( rb_ePGError, "unlink of oid %d failed", oid);
    return Qnil;
}

static void
free_pglarge( ptr)
    PGlarge *ptr;
{
    if (ptr->lo_fd > 0)
        lo_close( ptr->pgconn, ptr->lo_fd);
    free( ptr);
}

static VALUE
pglarge_new( conn, lo_oid, lo_fd)
    PGconn *conn;
    Oid lo_oid;
    int lo_fd;
{
    VALUE obj;
    PGlarge *pglarge;

    obj = Data_Make_Struct( rb_cPGLarge, PGlarge, 0, free_pglarge, pglarge);
    pglarge->pgconn = conn;
    pglarge->lo_oid = lo_oid;
    pglarge->lo_fd = lo_fd;

    return obj;
}

/*
 * call-seq:
 *    lrg.oid()
 *
 * Returns the large object's +oid+.
 */
static VALUE
pglarge_oid( obj)
    VALUE obj;
{
    PGlarge *pglarge;

    pglarge = get_pglarge( obj);
    return INT2NUM( pglarge->lo_oid);
}


static int
large_tell( pglarge)
    PGlarge *pglarge;
{
    int pos;

    pos = lo_tell( pglarge->pgconn, pglarge->lo_fd);
    if (pos == -1)
        rb_raise( rb_ePGError, "error while getting position");
    return pos;
}

static int
large_lseek( pglarge, offset, whence)
    PGlarge *pglarge;
    int offset, whence;
{
    int ret;

    ret = lo_lseek( pglarge->pgconn, pglarge->lo_fd, offset, whence);
    if (ret == -1)
        rb_raise( rb_ePGError, "error while moving cursor");
    return ret;
}

/*
 * call-seq:
 *    lrg.tell()
 *
 * Returns the current position of the large object pointer.
 */
static VALUE
pglarge_tell( obj)
    VALUE obj;
{
    return INT2NUM( large_tell( get_pglarge( obj)));
}

static VALUE
loread_all( obj)
    VALUE obj;
{
    PGlarge *pglarge;
    VALUE str;
    long bytes = 0;
    int n;

    pglarge = get_pglarge( obj);
    str = rb_tainted_str_new( 0, 0);
    for (;;) {
        rb_str_resize( str, bytes + BUFSIZ);
        n = lo_read( pglarge->pgconn, pglarge->lo_fd,
                     RSTRING( str)->ptr + bytes, BUFSIZ);
        bytes += n;
        if (n < BUFSIZ)
            break;
    }
    rb_str_resize( str, bytes);
    return str;
}

/*
 * call-seq:
 *    lrg.read( [length] )
 *
 * Attempts to read _length_ bytes from large object.
 * If no _length_ is given, reads all data.
 */
static VALUE
pglarge_read( argc, argv, obj)
    int argc;
    VALUE *argv;
    VALUE obj;
{
    int len;
    PGlarge *pglarge = get_pglarge( obj);
    VALUE str;

    VALUE length;
    rb_scan_args( argc, argv, "01", &length);
    if (NIL_P( length))
        return loread_all( obj);

    len = NUM2INT( length);
    if (len < 0)
        rb_raise( rb_ePGError, "nagative length %d given", len);

    str = rb_tainted_str_new( 0, len);
    len = lo_read( pglarge->pgconn, pglarge->lo_fd, STR2CSTR( str), len);
    if (len < 0)
        rb_raise( rb_ePGError, "error while reading");
    if (len == 0)
        return Qnil;
    else {
        RSTRING( str)->len = len;
        return str;
    }
}

/*
 * call-seq:
 *    lrg.write( str )
 *
 * Writes the string _str_ to the large object.
 * Returns the number of bytes written.
 */
static VALUE
pglarge_write( obj, buffer)
    VALUE obj, buffer;
{
    PGlarge *pglarge;
    int n;

    Check_Type( buffer, T_STRING);

    if (RSTRING( buffer)->len < 0)
        rb_raise( rb_ePGError, "write buffer zero string");

    pglarge = get_pglarge( obj);
    n = lo_write( pglarge->pgconn, pglarge->lo_fd,
                  STR2CSTR( buffer), RSTRING( buffer)->len);
    if (n == -1)
        rb_raise( rb_ePGError, "buffer truncated during write");

    return INT2FIX( n);
}

/*
 * call-seq:
 *    lrg.seek( offset, whence )
 *
 * Move the large object pointer to the _offset_.
 * Valid values for _whence_ are +SEEK_SET+, +SEEK_CUR+, and +SEEK_END+.
 * (Or 0, 1, or 2.)
 */
static VALUE
pglarge_seek( obj, offset, whence)
    VALUE obj, offset, whence;
{
    PGlarge *pglarge = get_pglarge( obj);
    return INT2NUM( large_lseek( pglarge, NUM2INT( offset), NUM2INT( whence)));
}

/*
 * call-seq:
 *    lrg.size()
 *
 * Returns the size of the large object.
 */
static VALUE
pglarge_size( obj)
    VALUE obj;
{
    PGlarge *pglarge = get_pglarge( obj);
    int start, end;

    start = large_tell( pglarge);
    end = large_lseek( pglarge, 0, SEEK_END);
    large_lseek( pglarge, start, SEEK_SET);
    return INT2NUM( end);
}

/*
 * call-seq:
 *    lrg.export( file )
 *
 * Saves the large object to a file.
 */
static VALUE
pglarge_export( obj, filename)
    VALUE obj, filename;
{
    PGlarge *pglarge = get_pglarge( obj);

    Check_Type( filename, T_STRING);

    if (!lo_export( pglarge->pgconn, pglarge->lo_oid, STR2CSTR( filename))) {
        pg_raise_exec( pglarge->pgconn);
    }
    return Qnil;
}

/*
 * call-seq:
 *    lrg.unlink()
 *
 * Deletes the large object.
 */
static VALUE
pglarge_unlink( obj)
    VALUE obj;
{
    PGlarge *pglarge = get_pglarge( obj);

    if (!lo_unlink( pglarge->pgconn, pglarge->lo_oid)) {
        pg_raise_exec( pglarge->pgconn);
    }
    DATA_PTR( obj) = 0;

    return Qnil;
}

static VALUE
pgrow_init( self, keys)
    VALUE self, keys;
{
    VALUE args[1] = { LONG2NUM( RARRAY( keys)->len) };
    rb_call_super( 1, args);
    rb_ivar_set( self, id_keys, keys);
    return self;
}

/*
 * call-seq:
 *   row.keys -> Array
 *
 * Column names.
 */
static VALUE
pgrow_keys( self)
    VALUE self;
{
    return rb_ivar_get( self, id_keys);
}

/*
 * call-seq:
 *   row.values -> row
 */
static VALUE
pgrow_values( self)
    VALUE self;
{
    return self;
}

/*
 * call-seq:
 *   row[position] -> value
 *   row[name] -> value
 *
 * Access elements of this row by column position or name.
 */
static VALUE
pgrow_aref( argc, argv, self)
    int argc;
    VALUE * argv;
    VALUE self;
{
    if (TYPE( argv[0]) == T_STRING) {
        VALUE keys = pgrow_keys( self);
        VALUE index = rb_funcall( keys, rb_intern( "index"), 1, argv[0]);
        if (index == Qnil) {
            rb_raise( rb_ePGError, "%s: field not found",
                            STR2CSTR( argv[0]));
        }
        else {
            return rb_ary_entry( self, NUM2INT( index));
        }
    }
    else {
        return rb_call_super( argc, argv);
    }
}

/*
 * call-seq:
 *   row.each_value { |value| block } -> row
 *
 * Iterate with values.
 */
static VALUE
pgrow_each_value( self)
    VALUE self;
{
    rb_ary_each( self);
    return self;
}

/*
 * call-seq:
 *   row.each_pair { |column_value_array| block } -> row
 *
 * Iterate with column, value pairs.
 */
static VALUE
pgrow_each_pair( self)
    VALUE self;
{
    VALUE keys = pgrow_keys( self);
    int i;
    for (i = 0; i < RARRAY( keys)->len; ++i) {
        rb_yield( rb_assoc_new( rb_ary_entry( keys, i),
                                                rb_ary_entry( self, i)));
    }
    return self;
}

/*
 * call-seq:
 *   row.each { |column, value| block } -> row
 *   row.each { |value| block } -> row
 *
 * Iterate with values or column, value pairs.
 */
static VALUE
pgrow_each( self)
    VALUE self;
{
    int arity = NUM2INT( rb_funcall( rb_block_proc(), rb_intern( "arity"), 0));
    if (arity == 2) {
        pgrow_each_pair( self);
    }
    else {
        pgrow_each_value( self);
    }
    return self;
}

/*
 * call-seq:
 *   row.each_key { |column| block } -> row
 *
 * Iterate with column names.
 */
static VALUE
pgrow_each_key( self)
    VALUE self;
{
    rb_ary_each( pgrow_keys( self));
    return self;
}

/*
 * call-seq:
 *   row.to_hash -> Hash
 *
 * Returns a +Hash+ of the row's values indexed by column name.
 * Equivalent to <tt>Hash [*row.keys.zip( row).flatten]</tt>
 */
static VALUE
pgrow_to_hash( self)
    VALUE self;
{
    VALUE result = rb_hash_new();
    VALUE keys = pgrow_keys( self);
    int i;
    for (i = 0; i < RARRAY( self)->len; ++i) {
        rb_hash_aset( result, rb_ary_entry( keys, i), rb_ary_entry( self, i));
    }
    return result;
}

/* Large Object support */

/********************************************************************
 *
 * Document-class: Pg::Conn
 *
 * The class to access PostgreSQL database.
 *
 * For example, to send query to the database on the localhost:
 *    require 'pgsql'
 *    conn = Pg::Conn.open('dbname' => 'test1')
 *    res  = conn.exec('select * from a')
 *
 * See the Pg::Result class for information on working with the results of a
 * query.
 */


/********************************************************************
 *
 * Document-class: Pg::Result
 *
 * The class to represent the query result tuples (rows).
 * An instance of this class is created as the result of every query.
 * You may need to invoke the #clear method of the instance when finished with
 * the result for better memory performance.
 */


/********************************************************************
 *
 * Document-class: Pg::Row
 *
 * Array subclass that provides hash-like behavior.
 */


/********************************************************************
 *
 * Document-class: Pg::Large
 *
 * The class to access large objects.
 * An instance of this class is created as the  result of
 * Pg::Conn#lo_import, Pg::Conn#lo_create, and Pg::Conn#lo_open.
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

    rb_ePGExecError = rb_define_class_under( rb_mPg, "ExecError", rb_ePGError);
    rb_ePGConnError = rb_define_class_under( rb_mPg, "ConnError", rb_ePGError);

    rb_ePGResError = rb_define_class_under( rb_mPg, "ResultError", rb_ePGError);
    rb_define_method( rb_ePGResError, "status", pgreserror_status, 0);
    rb_define_method( rb_ePGResError, "sqlstate", pgreserror_sqlst, 0);
    rb_define_alias( rb_ePGResError, "errcode", "sqlstate");
    rb_define_method( rb_ePGResError, "primary", pgreserror_primary, 0);
    rb_define_method( rb_ePGResError, "details", pgreserror_detail, 0);
    rb_define_method( rb_ePGResError, "hint", pgreserror_hint, 0);

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

    rb_define_const( rb_cPGConn, "CONNECTION_OK", INT2FIX( CONNECTION_OK));
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
    rb_define_method( rb_cPGConn, "lo_create", pgconn_locreate, -1);
    rb_define_alias( rb_cPGConn, "locreate", "lo_create");
    rb_define_method( rb_cPGConn, "lo_open", pgconn_loopen, -1);
    rb_define_alias( rb_cPGConn, "loopen", "lo_open");
    rb_define_method( rb_cPGConn, "lo_export", pgconn_loexport, 2);
    rb_define_alias( rb_cPGConn, "loexport", "lo_export");
    rb_define_method( rb_cPGConn, "lo_unlink", pgconn_lounlink, 1);
    rb_define_alias( rb_cPGConn, "lounlink", "lo_unlink");

    rb_cPGLarge = rb_define_class_under( rb_mPg, "Large", rb_cObject);
    rb_define_method( rb_cPGLarge, "oid", pglarge_oid, 0);
    rb_define_method( rb_cPGLarge, "close", pglarge_close, 0);
    rb_define_method( rb_cPGLarge, "read", pglarge_read, -1);
    rb_define_method( rb_cPGLarge, "write", pglarge_write, 1);
    rb_define_method( rb_cPGLarge, "seek", pglarge_seek, 2);
    rb_define_method( rb_cPGLarge, "tell", pglarge_tell, 0);
    rb_define_method( rb_cPGLarge, "size", pglarge_size, 0);
    rb_define_method( rb_cPGLarge, "export", pglarge_export, 1);
    rb_define_method( rb_cPGLarge, "unlink", pglarge_unlink, 0);

    rb_define_const( rb_cPGLarge, "INV_WRITE", INT2FIX( INV_WRITE));
    rb_define_const( rb_cPGLarge, "INV_READ", INT2FIX( INV_READ));
    rb_define_const( rb_cPGLarge, "SEEK_SET", INT2FIX( SEEK_SET));
    rb_define_const( rb_cPGLarge, "SEEK_CUR", INT2FIX( SEEK_CUR));
    rb_define_const( rb_cPGLarge, "SEEK_END", INT2FIX( SEEK_END));


    rb_cPGResult = rb_define_class_under( rb_mPg, "Result", rb_cObject);
    rb_include_module( rb_cPGResult, rb_mEnumerable);

    rb_define_const( rb_cPGResult, "EMPTY_QUERY", INT2FIX( PGRES_EMPTY_QUERY));
    rb_define_const( rb_cPGResult, "COMMAND_OK", INT2FIX( PGRES_COMMAND_OK));
    rb_define_const( rb_cPGResult, "TUPLES_OK", INT2FIX( PGRES_TUPLES_OK));
    rb_define_const( rb_cPGResult, "COPY_OUT", INT2FIX( PGRES_COPY_OUT));
    rb_define_const( rb_cPGResult, "COPY_IN", INT2FIX( PGRES_COPY_IN));
    rb_define_const( rb_cPGResult, "BAD_RESPONSE", INT2FIX( PGRES_BAD_RESPONSE));
    rb_define_const( rb_cPGResult, "NONFATAL_ERROR",
                                                INT2FIX( PGRES_NONFATAL_ERROR));
    rb_define_const( rb_cPGResult, "FATAL_ERROR", INT2FIX( PGRES_FATAL_ERROR));

    rb_define_method( rb_cPGResult, "status", pgresult_status, 0);
    rb_define_alias( rb_cPGResult, "result", "entries");
    rb_define_alias( rb_cPGResult, "rows", "entries");
    rb_define_method( rb_cPGResult, "each", pgresult_each, 0);
    rb_define_method( rb_cPGResult, "[]", pgresult_aref, -1);
    rb_define_method( rb_cPGResult, "fields", pgresult_fields, 0);
    rb_define_method( rb_cPGResult, "num_tuples", pgresult_num_tuples, 0);
    rb_define_method( rb_cPGResult, "num_fields", pgresult_num_fields, 0);
    rb_define_method( rb_cPGResult, "fieldname", pgresult_fieldname, 1);
    rb_define_method( rb_cPGResult, "fieldnum", pgresult_fieldnum, 1);
    rb_define_method( rb_cPGResult, "type", pgresult_type, 1);
    rb_define_method( rb_cPGResult, "size", pgresult_size, 1);
    rb_define_method( rb_cPGResult, "getvalue", pgresult_getvalue, 2);
    rb_define_method( rb_cPGResult, "getvalue_byname",
                                                 pgresult_getvalue_byname, 2);
    rb_define_method( rb_cPGResult, "getlength", pgresult_getlength, 2);
    rb_define_method( rb_cPGResult, "getisnull", pgresult_getisnull, 2);
    rb_define_method( rb_cPGResult, "cmdtuples", pgresult_cmdtuples, 0);
    rb_define_method( rb_cPGResult, "cmdstatus", pgresult_cmdstatus, 0);
    rb_define_method( rb_cPGResult, "oid", pgresult_oid, 0);
    rb_define_method( rb_cPGResult, "print", pgresult_print, 2);
    rb_define_method( rb_cPGResult, "clear", pgresult_clear, 0);
    rb_define_alias( rb_cPGResult, "close", "clear");

    rb_cPGRow = rb_define_class_under( rb_mPg, "Row", rb_cArray);
    rb_define_method( rb_cPGRow, "initialize", pgrow_init, 1);
    rb_define_method( rb_cPGRow, "[]", pgrow_aref, -1);
    rb_define_method( rb_cPGRow, "keys", pgrow_keys, 0);
    rb_define_method( rb_cPGRow, "values", pgrow_values, 0);
    rb_define_method( rb_cPGRow, "each", pgrow_each, 0);
    rb_define_method( rb_cPGRow, "each_pair", pgrow_each_pair, 0);
    rb_define_method( rb_cPGRow, "each_key", pgrow_each_key, 0);
    rb_define_method( rb_cPGRow, "each_value", pgrow_each_value, 0);
    rb_define_method( rb_cPGRow, "to_hash", pgrow_to_hash, 0);

    id_new        = rb_intern( "new");
    id_savepoints = rb_intern( "savepoints");
    id_on_notice  = rb_intern( "@keys");
    id_keys       = rb_intern( "@on_notice");
    id_gsub       = rb_intern( "gsub");
    id_gsub_bang  = rb_intern( "gsub!");

    pg_escape_regex = rb_reg_new( "([\\t\\n\\\\])", 10, 0);
    rb_global_variable( &pg_escape_regex);
    pg_escape_str = rb_str_new( "\\\\\\1", 4);
    rb_global_variable( &pg_escape_str);
}

