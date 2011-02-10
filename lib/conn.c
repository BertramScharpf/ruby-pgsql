/*
 *  conn.c  --  Pg connection
 */


#include "conn.h"

#include <st.h>
#include <intern.h>

#include "pgsql.h"
#include "result.h"


static ID id_to_postgres;
static ID id_iso8601;
static ID id_rows;
static ID id_call;
static ID id_on_notice;
static ID id_gsub_bang;

static VALUE pg_escape_regex;
static VALUE pg_escape_str;

static int   set_connect_params( st_data_t key, st_data_t val, st_data_t args);
static void  connstr_to_hash( VALUE params, VALUE str);
static void  connstr_passwd( VALUE self, VALUE params);

static VALUE pgconn_s_connect( int argc, VALUE *argv, VALUE klass);
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
static void notice_receiver( void *self, const PGresult *result);
static VALUE pgconn_on_notice( VALUE self);

static VALUE pgconn_socket( VALUE obj);

static VALUE pgconn_trace( VALUE obj, VALUE port);
static VALUE pgconn_untrace( VALUE obj);

static VALUE pgconn_escape_bytea( VALUE self, VALUE obj);
static VALUE pgconn_unescape_bytea( VALUE self, VALUE obj);
static int   needs_dquote_string( VALUE str);
static VALUE dquote_string( VALUE str);
static VALUE stringize_array( VALUE self, VALUE result, VALUE ary);
static VALUE pgconn_s_stringize( VALUE self, VALUE obj);
static VALUE pgconn_s_stringize_line( VALUE self, VALUE ary);

static VALUE pgconn_quote_bytea( VALUE self, VALUE obj);
static VALUE quote_string( VALUE conn, VALUE str);
static VALUE quote_array( VALUE self, VALUE result, VALUE ary);
static VALUE pgconn_quote( VALUE self, VALUE value);
static void  quote_all( VALUE self, VALUE ary, VALUE res);
static VALUE pgconn_quote_all( int argc, VALUE *argv, VALUE self);

static VALUE pgconn_client_encoding( VALUE obj);
static VALUE pgconn_set_client_encoding( VALUE obj, VALUE str);


static const char **params_to_strings( VALUE conn, VALUE params);
static VALUE exec_sql_statement( int argc, VALUE *argv, VALUE self);
static VALUE yield_or_return_result( VALUE res);
static VALUE pgconn_exec( int argc, VALUE *argv, VALUE obj);
static VALUE clear_resultqueue( VALUE obj);
static VALUE pgconn_send( int argc, VALUE *argv, VALUE obj);
static VALUE pgconn_fetch( VALUE obj);

static VALUE pgconn_query(         int argc, VALUE *argv, VALUE obj);
static VALUE pgconn_select_one(    int argc, VALUE *argv, VALUE self);
static VALUE pgconn_select_value(  int argc, VALUE *argv, VALUE self);
static VALUE pgconn_select_values( int argc, VALUE *argv, VALUE self);
static VALUE pgconn_get_notify( VALUE self);

static VALUE rescue_transaction( VALUE obj);
static VALUE yield_transaction( VALUE obj);
static VALUE pgconn_transaction( int argc, VALUE *argv, VALUE obj);
static VALUE rescue_subtransaction( VALUE obj);
static VALUE yield_subtransaction( VALUE obj);
static VALUE pgconn_subtransaction( int argc, VALUE *argv, VALUE obj);
static VALUE pgconn_transaction_status( VALUE obj);


static VALUE pgconn_loimport( VALUE obj, VALUE filename);
static VALUE pgconn_loexport( VALUE obj, VALUE lo_oid, VALUE filename);
static VALUE pgconn_lounlink( VALUE obj, VALUE lo_oid);
static VALUE pgconn_locreate( int argc, VALUE *argv, VALUE obj);
static VALUE pgconn_loopen( int argc, VALUE *argv, VALUE obj);


static const char *str_NULL = "NULL";



VALUE rb_cPgConn;

int translate_results = 1;


PGconn*
get_pgconn( obj)
    VALUE obj;
{
    PGconn *conn;

    Data_Get_Struct( obj, PGconn, conn);
    if (conn == NULL)
        rb_raise( rb_ePgError, "not a valid connection");
    return conn;
}

PGresult *pg_pqexec( PGconn *conn, const char *cmd)
{
    PGresult *result;

    result = PQexec( conn, cmd);
    if (result == NULL)
        pg_raise_exec( conn);
    pg_checkresult( conn, result);
    return result;
}


/*
 * Document-method: connect
 *
 * call-seq:
 *     Pg::Conn.connect( hash)        ->  conn
 *     Pg::Conn.connect( str, hash)   ->  conn
 *     Pg::Conn.connect( hash)      { |conn| ... }  -> obj
 *     Pg::Conn.connect( str, hash) { |conn| ... }  -> obj
 *
 * Without a block this is the same as +Pg::Conn.new+.  If a block is given,
 * the connection will be closed afterwards.
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

int set_connect_params( st_data_t key, st_data_t val, st_data_t args)
{
    const char ***ptrs = (const char ***)args;
    VALUE k, v;

    k = (VALUE) key;
    v = (VALUE) val;
    if (!NIL_P( v)) {
        switch(TYPE( k)) {
          case T_SYMBOL:
            *(ptrs[ 0]) = rb_id2name( SYM2ID( k));
            break;
          case T_STRING:
            *(ptrs[ 0]) = StringValueCStr( k);
            break;
          default:
            *(ptrs[ 0]) = RSTRING_PTR( rb_obj_as_string( k));
            break;
        }
        switch(TYPE( v)) {
          case T_STRING:
            *(ptrs[ 1]) = StringValueCStr( v);
            break;
          default:
            *(ptrs[ 1]) = RSTRING_PTR( rb_obj_as_string( v));
            break;
        }
        ptrs[ 0]++;
        ptrs[ 1]++;
    }
    return ST_CONTINUE;
}

static const char re_connstr[] =
    "\\A"
    "(?:(.*?)(?::(.*))?@)?"             /* user:passwd@     */
    "(?:(.*?)(?::(\\d+))?/)?(?:(.+))?"  /* host:port/dbname */
    "\\z"
;

#define KEY_USER     "user"
#define KEY_PASSWORD "password"
#define KEY_HOST     "host"
#define KEY_PORT     "port"
#define KEY_DBNAME   "dbname"

void connstr_to_hash( VALUE params, VALUE str)
{
    VALUE re, match;

    re = rb_reg_new( re_connstr, sizeof re_connstr - 1, 0);
    if (RTEST( rb_reg_match( re, str))) {
        match = rb_backref_get();
#define ADD_TO_RES( key, n) rb_hash_aset( params, \
            ID2SYM( rb_intern( key)), rb_reg_nth_match( n, match))
        ADD_TO_RES( KEY_USER,     1);
        ADD_TO_RES( KEY_PASSWORD, 2);
        ADD_TO_RES( KEY_HOST,     3);
        ADD_TO_RES( KEY_PORT,     4);
        ADD_TO_RES( KEY_DBNAME,   5);
#undef ADD_TO_RES
    } else
        rb_raise( rb_eArgError, "Unvalid connection: %s", RSTRING_PTR( str));
}

void connstr_passwd( VALUE self, VALUE params)
{
    VALUE id_password;
    VALUE pw, cl, repl;

    id_password = ID2SYM( rb_intern( KEY_PASSWORD));
    pw = rb_hash_aref( params, id_password);
    if (TYPE( pw) == T_STRING && RSTRING_LEN( pw) == 0) {
        VALUE id_password_q;

        id_password_q = rb_intern( "password?");
        if (rb_respond_to( self, id_password_q))
            repl = rb_funcall( self, id_password_q, 0);
        else {
            cl = CLASS_OF( self);
            if (rb_respond_to( cl, id_password_q))
                repl = rb_funcall( cl, id_password_q, 0);
            else
                repl = Qnil;
        }
        rb_hash_aset( params, id_password, repl);
    }
}

/*
 * Document-method: new
 *
 * call-seq:
 *     Pg::Conn.new( hash)        ->   conn
 *     Pg::Conn.new( str, hash)   ->   conn
 *
 * Establish a connection to a PostgreSQL server.
 * The parameters may be specified as a hash:
 *
 *   c = Pg::Conn.new :dbname => "movies", :host => "jupiter", ...
 *
 * The most common parameters may be given in a URL-like
 * connection string:
 *
 *   "user:password@host:port/dbname"
 *
 * Any of these parts may be omitted.  If there is no slash, the part after the
 * @ sign will be read as database name.
 *
 * If the password is the empty string, and there is either an instance method
 * or a class method <code>password?</code>, that method will be asked.
 *
 * See the PostgreSQL documentation for a full list:
 * [http://www.postgresql.org/docs/current/interactive/libpq-connect.html#LIBPQ-PQCONNECTDBPARAMS]
 *
 * On failure, a +Pg::Error+ exception will be raised.
 */
VALUE
pgconn_init( argc, argv, self)
    int argc;
    VALUE *argv;
    VALUE self;
{
    VALUE str, params;
    int l;
    const char **keywords, **values;
    const char **ptrs[ 2];
    PGconn *conn = NULL;

    if (rb_scan_args( argc, argv, "02", &str, &params) > 0) {
        if (TYPE( str) == T_STRING) {
            if (NIL_P( params))
                params = rb_hash_new();
            else
                params = rb_obj_dup( params);
            connstr_to_hash( params, str);
        } else
            params = str;
    } else
        params = rb_hash_new();
    connstr_passwd( self, params);

    l = RHASH_SIZE( params) + 1;
    keywords = (const char **) ALLOCA_N( char *, l);
    values   = (const char **) ALLOCA_N( char *, l);
    ptrs[ 0] = keywords;
    ptrs[ 1] = values;
    st_foreach( RHASH_TBL( params), set_connect_params, (st_data_t) ptrs);
    *(ptrs[ 0]) = *(ptrs[ 1]) = NULL;

    conn = PQconnectdbParams( keywords, values, 0);
    if (PQstatus( conn) == CONNECTION_BAD)
        pg_raise_conn( conn);
    Check_Type( self, T_DATA);
    DATA_PTR(self) = conn;
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
 * Resets the backend connection.  This method closes the backend connection
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
 * into two-decimal-digit numbers and appending them together.  For example,
 * version 7.4.2 will be returned as 70402, and version 8.1 will be returned as
 * 80100 (leading zeroes are not shown).  Zero is returned if the connection is
 * bad.
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

void
notice_receiver( self, result)
    void *self;
    const PGresult *result;
{
    VALUE block, err;

    block = rb_ivar_get( (VALUE) self, id_on_notice);
    if (block != Qnil) {
        err = pgreserror_new( (PGresult *) result);
        rb_funcall( block, id_call, 1, err);
        /* PQclear will be done by Postgres. */
    }
}

/*
 * call-seq:
 *   conn.on_notice { |message| ... }
 *
 * This method yields a PG::ResultError object in case a nonfatal exception
 * was raised.
 *
 * Here's an example:
 *
 *   conn.exec <<-EOT
 *     create or replace function noise() returns void as $$
 *     begin
 *       raise notice 'Hi!';
 *     end;
 *     $$ language plpgsql;
 *   EOT
 *   conn.on_notice { |e| puts e.inspect }
 *   conn.exec "select noise();"
 */
VALUE
pgconn_on_notice( self)
    VALUE self;
{
    PGconn *conn;

    conn = get_pgconn( self);
    if (PQsetNoticeReceiver( conn, NULL, NULL) != &notice_receiver) {
        if (!id_call)
            id_call = rb_intern( "call");
        PQsetNoticeReceiver( conn, &notice_receiver, (void *) self);
    }
    if (!id_on_notice)
        id_on_notice = rb_intern( "@on_notice");
    rb_ivar_set( self, id_on_notice, rb_block_proc());
    return self;
}




/*
 * call-seq:
 *    conn.socket()  -> io
 *
 * Returns the sockets IO object.
 */
VALUE
pgconn_socket( obj)
    VALUE obj;
{
    int fd;

    fd = PQsocket( get_pgconn( obj));
    return rb_funcall( rb_cIO, id_new, 1, INT2NUM( fd));
}


/*
 * call-seq:
 *    conn.trace( port)
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
    PQtrace( get_pgconn( obj), rb_io_stdio_file( fp));
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
 *   conn.escape_bytea( str)  -> str
 *
 * Converts a string of binary data into an escaped version.
 * Example:
 *
 *   conn.escape_bytea "abc"    # => "\\x616263"
 *                              # (One backslash, then an 'x'.)
 *
 * This is what you need, when you pass your object as a Conn#exec parameter,
 * as a +COPY+ input line or as a subject to +Conn#quote+-ing.
 *
 * If you execute an +INSERT+ statement and mention you object in the statement
 * string you should call Conn.quote_bytea().
 *
 * See the PostgreSQL documentation on PQescapeByteaConn
 * [http://www.postgresql.org/docs/current/interactive/libpq-exec.html#LIBPQ-PQESCAPEBYTEACONN]
 * for more information.
 */
VALUE
pgconn_escape_bytea( self, str)
    VALUE self, str;
{
    unsigned char *s;
    size_t l;
    char nib[ 3];
    VALUE ret;

    Check_Type( str, T_STRING);
    ret = rb_str_buf_new2( "\\x");
    for (s = (unsigned char *) RSTRING_PTR( str), l = RSTRING_LEN( str); l;
                            ++s, --l) {
        sprintf( nib, "%02x", (int) *s);
        rb_str_buf_cat( ret, nib, 2);
    }
    OBJ_INFECT( ret, str);
    return ret;
}

/*
 * call-seq:
 *   conn.unescape_bytea( str)  -> str
 *
 * Converts an escaped string representation of binary data into binary data.
 * Example:
 *
 *   Pg::Conn.unescape_bytea "\\x616263"   # =>  "abc"
 *
 * Normally you will not need this because Pg::Result's methods will convert a
 * return value automatically if the field type was a +bytea+.
 *
 * See the PostgreSQL documentation on PQunescapeBytea
 * [http://www.postgresql.org/docs/current/interactive/libpq-exec.html#LIBPQ-PQUNESCAPEBYTEA]
 * for more information.
 */
VALUE
pgconn_unescape_bytea( self, str)
    VALUE self, str;
{
    VALUE ret;

    Check_Type( str, T_STRING);
    ret = string_unescape_bytea( RSTRING_PTR( str));
    OBJ_INFECT( ret, str);
    return ret;
}

int
needs_dquote_string( str)
    VALUE str;
{
    char *p;
    long l;

    if (strcmp( RSTRING_PTR( str), str_NULL) == 0)
        return 1;
    l = RSTRING_LEN( str);
    if (l == 0)
        return 1;
    for (p = RSTRING_PTR( str); l; ++p, --l)
        if (*p == ',' || *p == ' ' || *p == '\\' || *p == '"')
            break;
    return l > 0;
}

VALUE
dquote_string( str)
    VALUE str;
{
    VALUE ret;

    ret = str;
    if (needs_dquote_string( str)) {
        char *p, *q;
        long l, m;

        ret = rb_str_buf_new2( "\"");
        p = RSTRING_PTR( str);
        l = RSTRING_LEN( str);
        while (l) {
            q = p, m = l;
            for (; m && (*q != '"' && *q != '\\'); --m, ++q)
                ;
            rb_str_buf_cat( ret, p, l - m);
            if (m) {
                rb_str_buf_cat2( ret, "\\");
                rb_str_buf_cat( ret, q, 1);
                --m, ++q;
            }
            p = q, l = m;
        }
        rb_str_buf_cat2( ret, "\"");
    }
    return ret;
}

VALUE
stringize_array( self, result, ary)
    VALUE self, result, ary;
{
    long i, j;
    VALUE *o;
    VALUE cf, co;
    VALUE r;

    cf = Qundef;
    for (o = RARRAY_PTR( ary), j = RARRAY_LEN( ary); j; ++o, --j) {
        co = CLASS_OF( *o);
        if (cf == Qundef)
            cf = co;
        else {
            if (co != cf)
                rb_raise( rb_ePgError, "Array members of different type.");
            rb_str_buf_cat2( result, ",");
        }
        r = pgconn_s_stringize( self, *o);
        if (!NIL_P( *o)) {
            r = dquote_string( r);
            OBJ_INFECT( result, *o);
        }
        rb_str_buf_append( result, r);
    }
    return result;
}

/*
 * call-seq:
 *   Pg::Conn.stringize( obj) -> str
 *
 * This methods makes a string out of everything.  Numbers, booleans, +nil+,
 * date and time values, and even arrays will be written as PostgreSQL accepts
 * constants.  You may pass the result as a parameter to +exec+, +query+ etc.
 * or as a field after a +COPY+ statement if you don't use the standard
 * delimiters.
 *
 * Any other objects will be checked whether they have a method named
 * +to_postgres+.  If that doesn't exist the object will be converted by +to_s+.
 *
 * If you are quoting into a SQL statement please don't do something like
 * <code>"insert into ... (E'#{Conn.stringize obj}', ...);"</code>.  Use
 * +Conn.quote+ instead that will put the appropriate quoting characters around
 * its string.
 *
 * If you like to pass a +bytea+ you have to escape the string yourself.
 * This library cannot decide itself whether a String object is meant as a
 * string or as a +bytea+.  See the Pg::Conn#escape_bytea method.
 */
VALUE
pgconn_s_stringize( self, obj)
    VALUE self, obj;
{
    VALUE result;

    switch (TYPE( obj)) {
        case T_STRING:
            result = obj;
            break;

        case T_NIL:
            result = rb_str_new2( str_NULL);
            break;

        case T_TRUE:
        case T_FALSE:
        case T_FIXNUM:
            result = rb_obj_as_string( obj);
            break;

        case T_BIGNUM:
        case T_FLOAT:
            result = rb_obj_as_string( obj);
            break;

        case T_ARRAY:
            result = rb_str_buf_new2( "{");
            stringize_array( self, result, obj);
            rb_str_buf_cat2( result, "}");
            break;

        default:
            if (rb_obj_is_kind_of( obj, rb_cNumeric))
                result = rb_obj_as_string( obj);
            else {
                VALUE co;

                co = CLASS_OF( obj);
                if        (co == rb_cTime) {
                    result = rb_funcall( obj, id_iso8601, 0);
                    OBJ_INFECT( result, obj);
                } else if (co == rb_cDate)
                    result = rb_obj_as_string( obj);
                else if   (co == rb_cDateTime)
                    result = rb_obj_as_string( obj);
                else if   (rb_respond_to( obj, id_to_postgres)) {
                    result = rb_funcall( obj, id_to_postgres, 0);
                    Check_Type( result, T_STRING);
                    OBJ_INFECT( result, obj);
                } else
                    result = rb_obj_as_string( obj);
            }
            break;
    }
    return result;
}

/*
 * call-seq:
 *    Pg::Conn.stringize_line( ary)  ->  str
 *
 * Quote a line the standard way that +COPY+ expects. Tabs, newlines, and
 * backslashes will be escaped, +nil+ will become "\\N".
 */
VALUE pgconn_s_stringize_line( self, ary)
    VALUE self, ary;
{
    VALUE *a;
    int l;
    VALUE ret, s;

    Check_Type( ary, T_ARRAY);

    ret = rb_str_new( NULL, 0);
    for (l = RARRAY_LEN( ary), a = RARRAY_PTR( ary); l; ++a) {
        if (NIL_P( *a))
            rb_str_cat( ret, "\\N", 2);
        else {
            s = pgconn_s_stringize( self, *a);
            rb_funcall( s, id_gsub_bang, 2, pg_escape_regex, pg_escape_str);
            rb_str_cat( ret, RSTRING_PTR( s), RSTRING_LEN( s));
        }
        if (--l > 0)
            rb_str_cat( ret, "\t", 1);
        else
            rb_str_cat( ret, "\n", 1);
    }
    return ret;
}




/*
 * call-seq:
 *   conn.quote_bytea( str)  -> str
 *
 * Converts a string of binary data into an escaped version.
 * Example:
 *
 *   conn.quote_bytea "abc"    # => "E'\\\\x616263'::bytea"
 *                             # (Two backslashes, then an 'x'.)
 *
 * This is what you need, when you execute an +INSERT+ statement and mention
 * you object in the statement string.
 *
 * If you pass your object as a Conn#exec parameter, as a +COPY+ input line or
 * as a subject to +Conn#quote+-ing you should call Conn.escape_bytea().
 *
 * See the PostgreSQL documentation on PQescapeByteaConn
 * [http://www.postgresql.org/docs/current/interactive/libpq-exec.html#LIBPQ-PQESCAPEBYTEACONN]
 * for more information.
 */
VALUE
pgconn_quote_bytea( self, str)
    VALUE self, str;
{
    char *t;
    size_t l;
    VALUE ret;

    Check_Type( str, T_STRING);
    t = (char *) PQescapeByteaConn( get_pgconn( self),
            (unsigned char *) RSTRING_PTR( str), RSTRING_LEN( str), &l);
    ret = rb_str_buf_new2( " E'");
    rb_str_buf_cat( ret, t, l - 1);
    rb_str_buf_cat2( ret, "'::bytea");
    PQfreemem( t);
    OBJ_INFECT( ret, str);
    return ret;
}


VALUE
quote_string( conn, str)
    VALUE conn, str;
{
    char *p;
    VALUE result;

    p = PQescapeLiteral( get_pgconn( conn),
                    RSTRING_PTR( str), RSTRING_LEN( str));
    result = rb_str_new2( p);
    PQfreemem( p);
    OBJ_INFECT( result, str);
    return result;
}

VALUE
quote_array( self, result, ary)
    VALUE self, result, ary;
{
    long i, j;
    VALUE *o;
    VALUE cf, co;

    cf = Qundef;
    for (o = RARRAY_PTR( ary), j = RARRAY_LEN( ary); j; ++o, --j) {
        co = CLASS_OF( *o);
        if (cf == Qundef)
            cf = co;
        else {
            if (co != cf)
                rb_raise( rb_ePgError, "Array members of different type.");
            rb_str_buf_cat2( result, ",");
        }
        rb_str_buf_append( result, pgconn_quote( self, *o));
    }
    return result;
}

/*
 * call-seq:
 *   Pg::Conn.quote( obj) -> str
 *
 * This methods makes a PostgreSQL constant out of everything.  You may mention
 * any result in a statement passed to Conn#exec.
 *
 * If you prefer to pass your objects as a parameter to +exec+, +query+ etc.
 * or as a field after a +COPY+ statement you should call Pg::Conn#stringize.
 *
 * This method is to prevent you from saying something like
 * <code>"insert into ... (E'#{Conn.stringize obj}', ...);"</code>.  It is
 * more efficient to say
 *
 *   conn.exec "insert into ... (#{conn.quote obj}, ...);"
 *
 * Your self-defined classes will be checked whether they have a method named
 * +to_postgres+.  If that doesn't exist the object will be converted by +to_s+.
 *
 * Call Pg::Conn#quote_bytea if you want to tell your string is a byte array.
 */
VALUE
pgconn_quote( self, obj)
    VALUE self, obj;
{
    VALUE result;

    switch (TYPE( obj)) {
        case T_STRING:
            return quote_string( self, obj);
        case T_NIL:
            return rb_str_new2( str_NULL);
        case T_TRUE:
        case T_FALSE:
        case T_FIXNUM:
            return rb_obj_as_string( obj);
        case T_BIGNUM:
        case T_FLOAT:
            return rb_obj_as_string( obj);

        case T_ARRAY:
            result = rb_str_buf_new2( "ARRAY[");
            quote_array( self, result, obj);
            rb_str_buf_cat2( result, "]");
            break;

        default:
            if (rb_obj_is_kind_of( obj, rb_cNumeric))
                result = rb_obj_as_string( obj);
            else {
                VALUE co;
                char *type;

                co = CLASS_OF( obj);
                if        (co == rb_cTime) {
                    result = rb_funcall( obj, id_iso8601, 0);
                    type = "timestamptz";
                } else if (co == rb_cDate) {
                    result = rb_obj_as_string( obj);
                    type = "date";
                } else if (co == rb_cDateTime) {
                    result = rb_obj_as_string( obj);
                    type = "timestamptz";
                } else if (rb_respond_to( obj, id_to_postgres)) {
                    result = rb_funcall( obj, id_to_postgres, 0);
                    Check_Type( result, T_STRING);
                    type = NULL;
                } else {
                    result = rb_obj_as_string( obj);
                    type = "unknown";
                }
                result = quote_string( self, result);
                if (type != NULL) {
                    rb_str_buf_cat2( result, "::");
                    rb_str_buf_cat2( result, type);
                }
                OBJ_INFECT( result, obj);
            }
            break;
    }
    return result;
}

void quote_all( self, ary, res)
    VALUE self, ary, res;
{
    VALUE *p;
    long len;

    for (p = RARRAY_PTR( ary), len = RARRAY_LEN( ary); len; len--, p++) {
        if (TYPE( *p) == T_ARRAY)
            quote_all( self, *p, res);
        else {
            if (RSTRING_LEN( res) > 0)
                rb_str_buf_cat2( res, ",");
            rb_str_buf_append( res, pgconn_quote( self, *p));
        }
    }
}

VALUE pgconn_quote_all( argc, argv, self)
    int argc;
    VALUE *argv;
    VALUE self;
{
    VALUE res;
    VALUE args;

    res = rb_str_new( NULL, 0);
    rb_scan_args( argc, argv, "0*", &args);
    quote_all( self, args, res);
    return res;
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
    if ((PQsetClientEncoding( get_pgconn( obj), RSTRING_PTR( str))) == -1)
        rb_raise( rb_ePgError, "Invalid encoding name %s", str);
    return Qnil;
}


const char **params_to_strings( conn, params)
    VALUE conn, params;
{
    VALUE *ptr;
    int len;
    const char **values, **v;

    ptr = RARRAY_PTR( params);
    len = RARRAY_LEN( params);
    values = ALLOCA_N( const char *, len);
    for (v = values; len; v++, ptr++, len--)
        *v = *ptr == Qnil ? NULL
                         : RSTRING_PTR( pgconn_s_stringize( conn, *ptr));
    return values;
}

VALUE
exec_sql_statement( argc, argv, self)
    int argc;
    VALUE *argv;
    VALUE self;
{
    PGconn *conn;
    PGresult *result;
    VALUE command, params;
    int len;

    conn = get_pgconn( self);
    rb_scan_args( argc, argv, "1*", &command, &params);
    Check_Type( command, T_STRING);
    len = RARRAY_LEN( params);
    result = len <= 0 ?
        PQexec( conn, RSTRING_PTR( command)) :
        PQexecParams( conn, RSTRING_PTR( command), len,
                        NULL, params_to_strings( self, params), NULL, NULL, 0);
    if (result == NULL)
        pg_raise_exec( conn);
    pg_checkresult( conn, result);
    return pgresult_new( conn, result);
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
 *    conn.exec( sql, *bind_values)  -> result
 *
 * Sends SQL query request specified by +sql+ to the PostgreSQL.
 * Returns a Pg::Result instance.
 *
 * +bind_values+ represents values for the PostgreSQL bind parameters found in
 * the +sql+.  PostgreSQL bind parameters are presented as $1, $1, $2, etc.
 */
VALUE
pgconn_exec( argc, argv, self)
    int argc;
    VALUE *argv;
    VALUE self;
{
    return yield_or_return_result( exec_sql_statement( argc, argv, self));
}


VALUE
clear_resultqueue( obj)
    VALUE obj;
{
    PGconn *conn;
    PGresult *result;

    conn = get_pgconn( obj);
    while ((result = PQgetResult( conn)) != NULL)
        PQclear( result);
    return Qnil;
}

/*
 * call-seq:
 *    conn.send( sql, *bind_values)   -> nil
 *
 * Sends an asynchronous SQL query request specified by +sql+ to the
 * PostgreSQL server.
 *
 * Use Pg::Conn#fetch to fetch the results after you waited for data.
 *
 *   Pg::Conn.connect do |conn|
 *     conn.send "select pg_sleep(3), * from t;" do
 *       ins = [ conn.socket]
 *       loop do
 *         r = IO.select ins, nil, nil, 0.5
 *         break if r
 *         puts Time.now
 *       end
 *       res = conn.fetch
 *       res.each { |w| puts w.inspect }
 *     end
 *   end
 */
VALUE
pgconn_send( argc, argv, self)
    int argc;
    VALUE *argv;
    VALUE self;
{
    PGconn *conn;
    int res;
    VALUE command, params;
    int len;

    rb_scan_args( argc, argv, "1*", &command, &params);
    Check_Type( command, T_STRING);
    len = RARRAY_LEN( params);
    conn = get_pgconn( self);
    if (len <= 0)
        res = PQsendQuery( conn, RSTRING_PTR( command));
    else
        res = PQsendQueryParams( conn, RSTRING_PTR( command), len,
                NULL, params_to_strings( self, params), NULL, NULL, 0);
    if (res <= 0)
        pg_raise_conn( conn);
    return rb_ensure( rb_yield, Qnil, clear_resultqueue, self);
}

/*
 * call-seq:
 *    conn.fetch()                   -> result or nil
 *    conn.fetch() { |result| ... }  -> obj
 *
 * Fetches the results of the proevious Pg::Conn#send call.
 * See there for an example.
 *
 * The result will be +nil+ if there are no more results.
 */
VALUE
pgconn_fetch( obj)
    VALUE obj;
{
    PGconn *conn;
    PGresult *result;
    VALUE res;

    conn = get_pgconn( obj);
    if (PQconsumeInput( conn) == 0)
        pg_raise_exec( conn);
    if (PQisBusy( conn) > 0)
        return Qnil;
    result = PQgetResult( conn);
    if (result == NULL)
        res = Qnil;
    else {
        pg_checkresult( conn, result);
        res = pgresult_new( conn, result);
    }
    return yield_or_return_result( res);
}



/*
 * call-seq:
 *    conn.query( sql, *bind_values)    -> rows
 *    conn.query( sql, *bind_values) { |row| ... }   -> int or nil
 *
 * This is almost the same as Pg::Conn#exec except that it will yield or return
 * rows skipping the result object.
 *
 * If given a block, the nonzero number of rows will be returned or nil
 * otherwise.
 */
VALUE
pgconn_query( argc, argv, self)
    int argc;
    VALUE *argv;
    VALUE self;
{
    VALUE result;

    result = exec_sql_statement( argc, argv, self);
    if (rb_block_given_p())
        return rb_ensure( pgresult_each, result, pgresult_clear, result);
    else {
        VALUE rows;

        if (!id_rows)
            id_rows = rb_intern( "rows");
        rows = rb_funcall( result, id_rows, 0);
        pgresult_clear( result);
        return rows;
    }
}

/*
 * call-seq:
 *   conn.select_one( query, *bind_values)
 *
 * Return the first row of the query results.
 * Equivalent to <code>conn.query( query, *bind_values).first</code>.
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

    res = exec_sql_statement( argc, argv, self);
    result = get_pgresult( res);
    row = PQntuples( result) ? fetch_pgrow( result, 0, fetch_fields( result))
                             : Qnil;
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
    PGresult *result;
    VALUE ret;

    res = exec_sql_statement( argc, argv, self);
    result = get_pgresult( res);
    ret = PQntuples( result) ? fetch_pgresult( result, 0, 0) : Qnil;
    pgresult_clear( res);
    return ret;
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
    VALUE res;
    PGresult *result;
    int n, m;
    VALUE ret;
    int i, j, k;

    res = exec_sql_statement( argc, argv, self);
    result = get_pgresult( res);
    n = PQntuples( result), m = PQnfields( result);
    ret = rb_ary_new2( n * m);
    for (i = 0, k = 0; n; ++i, --n) {
        for (j = 0; m; ++j, --m, ++k)
            rb_ary_store( ret, k, fetch_pgresult( result, i, j));
        m = j;
    }
    pgresult_clear( res);
    return ret;
}

/*
 * call-seq:
 *    conn.get_notify()  -> ary or nil
 *
 * Returns a notifier. If there is no unprocessed notifier, it returns +nil+.
 */
VALUE
pgconn_get_notify( self)
    VALUE self;
{
    PGconn *conn;
    PGnotify *notify;
    VALUE ret;

    conn = get_pgconn( self);
    if (PQconsumeInput( conn) == 0)
        pg_raise_exec( conn);
    notify = PQnotifies( conn);
    if (notify == NULL)
        return Qnil;
    ret = rb_ary_new3( 3, rb_tainted_str_new2( notify->relname),
                          INT2NUM( notify->be_pid),
                          rb_tainted_str_new2( notify->extra));
    PQfreemem( notify);
    return ret;
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
 * Open and close a transaction block.  The isolation level will be
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
    if (!NIL_P( ser)) {
        rb_str_buf_cat2( cmd, " isolation level ");
        rb_str_buf_cat2( cmd, (RTEST(ser) ? "serializable" : "read committed"));
        p++;
    }
    if (!NIL_P( ro)) {
        if (p) rb_str_buf_cat2( cmd, ",");
        rb_str_buf_cat2( cmd, " read ");
        rb_str_buf_cat2( cmd, (RTEST(ro)  ? "only" : "write"));
    }
    rb_str_buf_cat2( cmd, ";");
    pg_pqexec( get_pgconn( self), RSTRING_PTR(cmd));
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
    pg_pqexec( get_pgconn( rb_ary_entry( ary, 0)), RSTRING_PTR(cmd));

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
    pg_pqexec( get_pgconn( rb_ary_entry( ary, 0)), RSTRING_PTR(cmd));

    return r;
}

/*
 * call-seq:
 *    conn.subtransaction( nam, *args) { |conn,sp| ... }
 *
 * Open and close a transaction savepoint.  The savepoints name +nam+ may
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
    pg_pqexec( get_pgconn( self), RSTRING_PTR(cmd));

    ya = rb_ary_new3( 2, self, sp);
    return rb_rescue( yield_subtransaction, ya, rescue_subtransaction, ya);
}

/*
 * call-seq:
 *    conn.transaction_status()  ->  int
 *
 * returns one of the following statuses:
 *
 *   PQTRANS_IDLE    = 0 (connection idle)
 *   PQTRANS_ACTIVE  = 1 (command in progress)
 *   PQTRANS_INTRANS = 2 (idle, within transaction block)
 *   PQTRANS_INERROR = 3 (idle, within failed transaction)
 *   PQTRANS_UNKNOWN = 4 (cannot determine status)
 */
VALUE
pgconn_transaction_status( obj)
    VALUE obj;
{
    return INT2NUM( PQtransactionStatus( get_pgconn( obj)));
}




/*
 * call-seq:
 *    conn.lo_import( file) -> oid
 *
 * Import a file to a large object.  Returns an oid on success.  On
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
    lo_oid = lo_import( conn, RSTRING_PTR( filename));
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
        rb_raise( rb_ePgError, "invalid large object oid %d", oid);

    conn = get_pgconn( obj);
    if (!lo_export( conn, oid, RSTRING_PTR( filename)))
        pg_raise_exec( conn);
    return Qnil;
}


/*
 * call-seq:
 *    conn.lo_create( [mode] ) -> Pg::Large
 *    conn.lo_create( [mode] ) { |pglarge| ... } -> oid
 *
 * Returns a Pg::Large instance on success.  On failure, it raises Pg::Error
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
 * Open a large object of _oid_.  Returns a Pg::Large instance on success.
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
        rb_raise( rb_ePgError, "invalid oid %d", oid);
    result = lo_unlink( get_pgconn( obj), oid);
    if (result < 0)
        rb_raise( rb_ePgError, "unlink of oid %d failed", oid);
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
#define CONN_DEF( c) rb_define_const( rb_cPgConn, #c, INT2FIX( c))

    rb_cPgConn = rb_define_class_under( rb_mPg, "Conn", rb_cObject);

    rb_define_alloc_func( rb_cPgConn, pgconn_alloc);
    rb_define_singleton_method( rb_cPgConn, "connect", pgconn_s_connect, -1);
    rb_define_alias( rb_singleton_class( rb_cPgConn), "open", "connect");

    rb_define_singleton_method( rb_cPgConn, "translate_results=",
                                           pgconn_s_translate_results_set, 1);

    CONN_DEF( CONNECTION_OK);
    CONN_DEF( CONNECTION_BAD);

    rb_define_method( rb_cPgConn, "initialize", pgconn_init, -1);
    rb_define_method( rb_cPgConn, "close", pgconn_close, 0);
    rb_define_alias( rb_cPgConn, "finish", "close");
    rb_define_method( rb_cPgConn, "reset", pgconn_reset, 0);
    rb_define_method( rb_cPgConn, "protocol_version",
                                                   pgconn_protocol_version, 0);
    rb_define_method( rb_cPgConn, "server_version", pgconn_server_version, 0);
    rb_define_method( rb_cPgConn, "db", pgconn_db, 0);
    rb_define_alias( rb_cPgConn, "dbname", "db");
    rb_define_method( rb_cPgConn, "host", pgconn_host, 0);
    rb_define_method( rb_cPgConn, "options", pgconn_options, 0);
    rb_define_method( rb_cPgConn, "port", pgconn_port, 0);
    rb_define_method( rb_cPgConn, "tty", pgconn_tty, 0);
    rb_define_method( rb_cPgConn, "user", pgconn_user, 0);
    rb_define_method( rb_cPgConn, "status", pgconn_status, 0);
    rb_define_method( rb_cPgConn, "error", pgconn_error, 0);
    rb_define_method( rb_cPgConn, "on_notice", pgconn_on_notice, 0);

    rb_define_method( rb_cPgConn, "socket", pgconn_socket, 0);

    rb_define_method( rb_cPgConn, "trace", pgconn_trace, 1);
    rb_define_method( rb_cPgConn, "untrace", pgconn_untrace, 0);

    rb_define_method( rb_cPgConn, "escape_bytea",
                                            pgconn_escape_bytea, 1);
    rb_define_singleton_method( rb_cPgConn, "escape_bytea",
                                            pgconn_escape_bytea, 1);
    rb_define_method( rb_cPgConn, "unescape_bytea",
                                            pgconn_unescape_bytea, 1);
    rb_define_singleton_method( rb_cPgConn, "unescape_bytea",
                                            pgconn_unescape_bytea, 1);
    rb_define_singleton_method( rb_cPgConn, "stringize",
                                            pgconn_s_stringize, 1);
    rb_define_method( rb_cPgConn, "stringize",
                                            pgconn_s_stringize, 1);
    rb_define_singleton_method( rb_cPgConn, "stringize_line",
                                            pgconn_s_stringize_line, 1);
    rb_define_method( rb_cPgConn, "stringize_line", pgconn_s_stringize_line, 1);

    rb_define_method( rb_cPgConn, "quote_bytea",
                                            pgconn_quote_bytea, 1);
    rb_define_method( rb_cPgConn, "quote", pgconn_quote, 1);
    rb_define_method( rb_cPgConn, "quote_all", pgconn_quote_all, -1);
    rb_define_alias( rb_cPgConn, "q", "quote_all");

    rb_define_method( rb_cPgConn, "client_encoding", pgconn_client_encoding, 0);
    rb_define_method( rb_cPgConn, "set_client_encoding",
                                               pgconn_set_client_encoding, 1);

    rb_define_method( rb_cPgConn, "exec", pgconn_exec, -1);
    rb_define_method( rb_cPgConn, "send", pgconn_send, -1);
    rb_define_method( rb_cPgConn, "fetch", pgconn_fetch, 0);

    rb_define_method( rb_cPgConn, "query", pgconn_query, -1);
    rb_define_method( rb_cPgConn, "select_one", pgconn_select_one, -1);
    rb_define_method( rb_cPgConn, "select_value", pgconn_select_value, -1);
    rb_define_method( rb_cPgConn, "select_values", pgconn_select_values, -1);
    rb_define_method( rb_cPgConn, "get_notify", pgconn_get_notify, 0);



    CONN_DEF( PQTRANS_IDLE);
    CONN_DEF( PQTRANS_ACTIVE);
    CONN_DEF( PQTRANS_INTRANS);
    CONN_DEF( PQTRANS_INERROR);
    CONN_DEF( PQTRANS_UNKNOWN);
    rb_define_method( rb_cPgConn, "transaction", pgconn_transaction, -1);
    rb_define_method( rb_cPgConn, "subtransaction", pgconn_subtransaction, -1);
    rb_define_alias( rb_cPgConn, "savepoint", "subtransaction");
    rb_define_method( rb_cPgConn, "transaction_status",
                                                 pgconn_transaction_status, 0);



    rb_define_method( rb_cPgConn, "insert_table", pgconn_insert_table, 2);
    rb_define_method( rb_cPgConn, "putline", pgconn_putline, 1);
    rb_define_method( rb_cPgConn, "getline", pgconn_getline, 0);
    rb_define_method( rb_cPgConn, "endcopy", pgconn_endcopy, 0);



    rb_define_method( rb_cPgConn, "lo_import", pgconn_loimport, 1);
    rb_define_alias( rb_cPgConn, "loimport", "lo_import");
    rb_define_method( rb_cPgConn, "lo_export", pgconn_loexport, 2);
    rb_define_alias( rb_cPgConn, "loexport", "lo_export");
    rb_define_method( rb_cPgConn, "lo_unlink", pgconn_lounlink, 1);
    rb_define_alias( rb_cPgConn, "lounlink", "lo_unlink");
    rb_define_method( rb_cPgConn, "lo_create", pgconn_locreate, -1);
    rb_define_alias( rb_cPgConn, "locreate", "lo_create");
    rb_define_method( rb_cPgConn, "lo_open", pgconn_loopen, -1);
    rb_define_alias( rb_cPgConn, "loopen", "lo_open");

    id_to_postgres = rb_intern( "to_postgres");
    id_iso8601     = rb_intern( "iso8601");
    id_rows        = 0;
    id_call        = 0;
    id_on_notice   = 0;
    id_gsub_bang   = rb_intern( "gsub!");

    pg_escape_regex = rb_reg_new( "([\\t\\n\\\\])", 10, 0);
    rb_global_variable( &pg_escape_regex);
    pg_escape_str = rb_str_new( "\\\\\\1", 4);
    rb_global_variable( &pg_escape_str);

#undef CONN_DEF
}

