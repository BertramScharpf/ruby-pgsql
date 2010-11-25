/*
 *  pgsql.c  --  Pg::Xxx classes
 */

#include "pgsql.h"

#include "large.h"
#include "row.h"
#include "result.h"
#include "conn.h"

#include <st.h>
#include <intern.h>


static VALUE rb_cBigDecimal;
static VALUE rb_cDate;      /* TODO: eliminiere doppeltes Vorkommen. */
static VALUE rb_cDateTime;  /* TODO: eliminiere doppeltes Vorkommen. */

static ID id_new;
static ID id_on_notice;
static ID id_gsub;
static ID id_gsub_bang;

static VALUE pg_escape_regex;
static VALUE pg_escape_str;



static VALUE format_single_element( VALUE obj);
static VALUE pgconn_s_format( VALUE self, VALUE obj);
static VALUE format_array_element( VALUE obj);
static VALUE yield_or_return_result( VALUE res);


static VALUE rescue_transaction( VALUE obj);
static VALUE yield_transaction( VALUE obj);
static VALUE rescue_subtransaction( VALUE obj);
static VALUE yield_subtransaction( VALUE obj);

static void notice_proxy( void *self, const char *message);

static VALUE pgconn_lastval( VALUE obj);

static int has_numeric_scale( int typmod);




static PGresult *pg_pqexec( PGconn *conn, const char *cmd);



PGresult *pg_pqexec( PGconn *conn, const char *cmd)
{
    PGresult *result;

    result = PQexec( conn, cmd);
    pg_checkresult( conn, result);
    return result;
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


void
Init_pgsql( void)
{
    rb_require( "bigdecimal");
    rb_cBigDecimal = rb_const_get( rb_cObject, rb_intern( "BigDecimal"));

    init_pg_module();
    init_pg_large();
    init_pg_row();
    init_pg_result();
    init_pg_conn();

    id_new        = rb_intern( "new");
    id_on_notice  = rb_intern( "@on_notice");
    id_gsub       = rb_intern( "gsub");
    id_gsub_bang  = rb_intern( "gsub!");

    pg_escape_regex = rb_reg_new( "([\\t\\n\\\\])", 10, 0);
    rb_global_variable( &pg_escape_regex);
    pg_escape_str = rb_str_new( "\\\\\\1", 4);
    rb_global_variable( &pg_escape_str);
}

