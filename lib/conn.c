/*
 *  conn.c  --  PostgreSQL connection
 */


#include "conn.h"

#include "result.h"

#if defined( HAVE_HEADER_ST_H)
    #include <st.h>
#elif defined( HAVE_HEADER_RUBY_ST_H)
    #include <ruby/st.h>
#endif
#if defined( HAVE_HEADER_INTERN_H)
    #include <intern.h>
#elif defined( HAVE_HEADER_RUBY_INTERN_H)
    #include <ruby/intern.h>
#endif

#if defined( HAVE_HEADER_RUBY_H)
    #define RB_ERRINFO ruby_errinfo
#elif defined( HAVE_HEADER_RUBY_RUBY_H)
    #define RB_ERRINFO (rb_errinfo())
#endif

#ifdef HAVE_HEADER_LIBPQ_LIBPQ_FS_H
    #include <libpq/libpq-fs.h>
#endif

static ID id_to_postgres;
static ID id_raw;
static ID id_format;
static ID id_iso8601;
static ID id_rows;
static ID id_call;
static ID id_on_notice;
static ID id_gsub_bang;
static id_command;
static id_parameters;

static VALUE pg_escape_regex;

static PGresult *pg_pqexec( PGconn *conn, VALUE cmd);

static VALUE pgconn_s_connect( int argc, VALUE *argv, VALUE cls);
static VALUE pgconn_s_parse( VALUE cls, VALUE str);
static VALUE pgconn_s_translate_results_set( VALUE cls, VALUE fact);

static int   set_connect_params( st_data_t key, st_data_t val, st_data_t args);
static void  connstr_to_hash( VALUE params, VALUE str);
static void  connstr_passwd( VALUE self, VALUE params);

static VALUE pgconn_alloc( VALUE cls);
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

static VALUE pgconn_format( VALUE self, VALUE obj);
static VALUE pgconn_escape_bytea( VALUE self, VALUE obj);
static VALUE pgconn_unescape_bytea( VALUE self, VALUE obj);
static int   needs_dquote_string( VALUE str);
static VALUE dquote_string( VALUE str);
static VALUE stringize_array( VALUE self, VALUE result, VALUE ary);
static VALUE pgconn_stringize( VALUE self, VALUE obj);
static VALUE gsub_escape_i( VALUE c, VALUE arg);
static VALUE pgconn_stringize_line( VALUE self, VALUE ary);

static VALUE pgconn_quote_bytea( VALUE self, VALUE obj);
static VALUE quote_string( VALUE conn, VALUE str);
static VALUE quote_array( VALUE self, VALUE result, VALUE ary);
static VALUE pgconn_quote( VALUE self, VALUE value);
static void  quote_all( VALUE self, VALUE ary, VALUE res);
static VALUE pgconn_quote_all( int argc, VALUE *argv, VALUE self);
static VALUE pgconn_quote_identifier( VALUE self, VALUE value);

static VALUE pgconn_client_encoding( VALUE obj);
static VALUE pgconn_set_client_encoding( VALUE obj, VALUE str);


static char **params_to_strings( VALUE conn, VALUE params);
static void free_strings( char **strs, int len);
static VALUE exec_sql_statement( int argc, VALUE *argv, VALUE self, int store);
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

static VALUE rescue_transaction( VALUE self);
static VALUE yield_transaction( VALUE self);
static VALUE pgconn_transaction( int argc, VALUE *argv, VALUE self);
static VALUE rescue_subtransaction( VALUE ary);
static VALUE yield_subtransaction( VALUE ary);
static VALUE pgconn_subtransaction( int argc, VALUE *argv, VALUE self);
static VALUE pgconn_transaction_status( VALUE self);

static VALUE put_end( VALUE conn);
static VALUE pgconn_copy_stdin( int argc, VALUE *argv, VALUE self);
static VALUE pgconn_putline( VALUE self, VALUE str);
static VALUE get_end( VALUE conn);
static VALUE pgconn_copy_stdout( int argc, VALUE *argv, VALUE self);
static VALUE pgconn_getline( int argc, VALUE *argv, VALUE self);
static VALUE pgconn_each_line( VALUE self);

static void pgconn_cmd_set( VALUE self, VALUE command, VALUE params);
static void pgconn_cmd_clear( VALUE self);

static void  pg_raise_pgres( VALUE self, PGresult *result);


static const char *str_NULL = "NULL";

static VALUE rb_cPgConn;
static VALUE rb_ePgConnectFailed;
static VALUE rb_ePgConnError;
static VALUE rb_ePgTransError;


int translate_results = 1;


PGresult *
pg_pqexec( conn, cmd)
    PGconn *conn;
    VALUE cmd;
{
    PGresult *result;

    result = PQexec( conn, RSTRING_PTR(cmd));
    if (result == NULL)
        pg_raise_pgconn( conn);
    if (pg_checkresult( result) < 0)
        rb_exc_raise( pgreserror_new( result, cmd, Qnil));
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
pgconn_s_connect( argc, argv, cls)
    int argc;
    VALUE *argv;
    VALUE cls;
{
    VALUE pgconn;

    pgconn = rb_class_new_instance( argc, argv, cls);
    return rb_block_given_p() ?
        rb_ensure( rb_yield, pgconn, pgconn_close, pgconn) : pgconn;
}


/*
 * call-seq:
 *   Pg::Conn.parse( str)    -> hash
 *
 * Parse a connection string and return a hash with keys <code>:dbname</code>,
 * <code>:user</code>, <code>:host</code>, etc.
 *
 */
VALUE
pgconn_s_parse( cls, str)
    VALUE cls, str;
{
    VALUE params;

    params = rb_hash_new();
    connstr_to_hash( params, rb_obj_as_string( str));
    return params;
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
pgconn_s_translate_results_set( cls, fact)
    VALUE cls, fact;
{
    translate_results = RTEST( fact) ? 1 : 0;
    return Qnil;
}


int
set_connect_params( st_data_t key, st_data_t val, st_data_t args)
{
    const char ***ptrs = (const char ***)args;
    VALUE k, v;

    k = (VALUE) key;
    v = (VALUE) val;
    if (!NIL_P( v)) {
        switch(TYPE( k)) {
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

void
connstr_to_hash( VALUE params, VALUE str)
{
    VALUE re, match, m;

    re = rb_reg_new( re_connstr, sizeof re_connstr - 1, 0);
    if (RTEST( rb_reg_match( re, str))) {
        match = rb_backref_get();
#define ADD_TO_RES( key, n) \
    m = rb_reg_nth_match( n, match); \
    if (!NIL_P( m)) rb_hash_aset( params, ID2SYM( rb_intern( key)), m)
        ADD_TO_RES( KEY_USER,     1);
        ADD_TO_RES( KEY_PASSWORD, 2);
        ADD_TO_RES( KEY_HOST,     3);
        ADD_TO_RES( KEY_PORT,     4);
        ADD_TO_RES( KEY_DBNAME,   5);
#undef ADD_TO_RES
    } else
        rb_raise( rb_eArgError, "Invalid connection: %s", RSTRING_PTR( str));
}

void
connstr_passwd( VALUE self, VALUE params)
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


VALUE
pgconn_alloc( cls)
    VALUE cls;
{
    return Data_Wrap_Struct( cls, 0, &PQfinish, NULL);
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

    if (rb_scan_args( argc, argv, "02", &str, &params) < 2)
        if (TYPE( str) != T_STRING) {
            params = str;
            str = Qnil;
        }
    if      (NIL_P( params))
        params = rb_hash_new();
    else if (TYPE( params) != T_HASH)
        params = rb_convert_type( params, T_HASH, "Hash", "to_hash");
    else
        params = rb_obj_dup( params);
    if (!NIL_P( str))
        connstr_to_hash( params, rb_obj_as_string( str));
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
        rb_raise( rb_ePgConnectFailed, PQerrorMessage( conn));
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
        err = pgreserror_new( (PGresult *) result, Qnil, Qnil);
        rb_funcall( block, id_call, 1, err);
        /* PQclear will be done by Postgres. */
    }
}

/*
 * call-seq:
 *   conn.on_notice { |message| ... }
 *
 * This method yields a PG::Result::Error object in case a nonfatal exception
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
 *    conn.trace( port) { ... }
 *
 * Enables tracing message passing between backend.
 * The trace message will be written to the _port_ object,
 * which is an instance of the class +File+ (or at least +IO+).
 *
 * In case a block is given +untrace+ will be called automatically.
 */
VALUE
pgconn_trace( self, port)
    VALUE self, port;
{
#if defined( HAVE_HEADER_RUBYIO_H)
    OpenFile* fp;
    #define rb_io_stdio_file GetWriteFile
#elif defined( HAVE_HEADER_RUBY_IO_H)
    rb_io_t *fp;
#endif

    if (TYPE( port) != T_FILE)
        rb_raise( rb_eArgError, "Not an IO object: %s",
                                StringValueCStr( port));
    GetOpenFile( port, fp);
    PQtrace( get_pgconn( self), rb_io_stdio_file( fp));
    return RTEST( rb_block_given_p()) ?
        rb_ensure( rb_yield, Qnil, pgconn_untrace, self) : Qnil;
}

/*
 * call-seq:
 *    conn.untrace()
 *
 * Disables the message tracing.
 */
VALUE
pgconn_untrace( self)
    VALUE self;
{
    PQuntrace( get_pgconn( self));
    return Qnil;
}


/*
 * call-seq:
 *   conn.format( obj)  -> obj
 *
 * Format an object before it will be made a PostgreSQL type.
 * By default this just returns the unmodified object but you may
 * overwrite it fitting you own needs.
 *
 * The Object won't be replaced if this method returns +nil+.
 *
 * = Example
 *
 *   class MyConn < Pg::Conn
 *     def format obj
 *       case obj
 *         when Currency then obj.to_s_by_locale
 *         else               obj
 *       end
 *     end
 *   end
 */
VALUE
pgconn_format( self, obj)
    VALUE self, obj;
{
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

    StringValue( str);
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

    StringValue( str);
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
        r = pgconn_stringize( self, *o);
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
 *   conn.stringize( obj) -> str
 *
 * This methods makes a string out of everything.  Numbers, booleans, +nil+,
 * date and time values, and even arrays will be written a string as PostgreSQL
 * accepts constants.  You may pass the result as a field after a +COPY+
 * statement. This will be called internally for the parameters to +exec+,
 * +query+ etc.
 *
 * Any other objects will be checked whether they have a method named
 * +to_postgres+.  If that doesn't exist the object will be converted by +to_s+.
 *
 * If you are quoting into a SQL statement please don't do something like
 * <code>"insert into ... (E'#{conn.stringize obj}', ...);"</code>.  Use
 * +Conn.quote+ instead that will put the appropriate quoting characters around
 * its string.
 *
 * If you like to pass a +bytea+ you have to escape the string yourself.
 * This library cannot decide itself whether a String object is meant as a
 * string or as a +bytea+.  See the Pg::Conn#escape_bytea method.
 */
VALUE
pgconn_stringize( self, obj)
    VALUE self, obj;
{
    VALUE o, result;

    o = rb_funcall( self, id_format, 1, obj);
    if (!NIL_P( o))
        obj = o;
    switch (TYPE( obj)) {
        case T_STRING:
            result = obj;
            break;

        case T_NIL:
            result = rb_str_new2( str_NULL);
            break;

        case T_TRUE:
        case T_FALSE:
            result = rb_obj_as_string( obj);
            break;

        case T_BIGNUM:
        case T_FLOAT:
        case T_FIXNUM:
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
                else if   (co == pg_currency_class() &&
                                    rb_respond_to( obj, id_raw))
                    result = rb_funcall( obj, id_raw, 0);
                else if   (rb_respond_to( obj, id_to_postgres)) {
                    result = rb_funcall( obj, id_to_postgres, 0);
                    StringValue( result);
                    OBJ_INFECT( result, obj);
                } else
                    result = rb_obj_as_string( obj);
            }
            break;
    }
    return result;
}


VALUE
gsub_escape_i( VALUE c, VALUE arg)
{
    const char *r;

    r = NULL;
    switch (*RSTRING_PTR( c)) {
        case '\b': r = "\\b";  break;
        case '\f': r = "\\f";  break;
        case '\n': r = "\\n";  break;
        case '\r': r = "\\r";  break;
        case '\t': r = "\\t";  break;
        case '\v': r = "\\v";  break;
        case '\\': r = "\\\\"; break;
        default:               break;
    }
    return rb_str_new2( r);
}

/*
 * call-seq:
 *    conn.stringize_line( ary)  ->  str
 *
 * Quote a line the standard way that +COPY+ expects. Tabs, newlines, and
 * backslashes will be escaped, +nil+ will become "\\N".
 */
VALUE
pgconn_stringize_line( self, ary)
    VALUE self, ary;
{
    VALUE a;
    VALUE *p;
    int l;
    VALUE ret, s;

    a = rb_check_convert_type( ary, T_ARRAY, "Array", "to_ary");
    if (NIL_P(a))
        rb_raise( rb_eArgError, "Give me an array.");
    if (NIL_P( pg_escape_regex)) {
        pg_escape_regex = rb_reg_new( "([\\b\\f\\n\\r\\t\\v\\\\])", 18, 0);
        rb_global_variable( &pg_escape_regex);
    }
    ret = rb_str_new( NULL, 0);
    for (l = RARRAY_LEN( a), p = RARRAY_PTR( a); l; ++p) {
        if (NIL_P( *p))
            rb_str_cat( ret, "\\N", 2);
        else {
            s = pgconn_stringize( self, *p);
            rb_block_call( s, id_gsub_bang, 1, &pg_escape_regex, gsub_escape_i, Qnil);
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

    StringValue( str);
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
 *   conn.quote( obj) -> str
 *
 * This methods makes a PostgreSQL constant out of everything.  You may mention
 * any result in a statement passed to Conn#exec.
 *
 * If you prefer to pass your objects as a parameter to +exec+, +query+ etc.
 * or as a field after a +COPY+ statement you should call conn#stringize.
 *
 * This method is to prevent you from saying something like
 * <code>"insert into ... (E'#{conn.stringize obj}', ...);"</code>.  It is
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
    VALUE o, result;

    o = rb_funcall( self, id_format, 1, obj);
    if (!NIL_P( o))
        obj = o;
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
                } else if (co == pg_currency_class() &&
                                    rb_respond_to( obj, id_raw)) {
                    result = rb_funcall( obj, id_raw, 0);
                    StringValue( result);
                    type = "money";
                } else if (rb_respond_to( obj, id_to_postgres)) {
                    result = rb_funcall( obj, id_to_postgres, 0);
                    StringValue( result);
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

void
quote_all( self, ary, res)
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

/*
 * call-seq:
 *   conn.quote_all( *args) -> str
 *
 * Does a #quote for every argument and pastes the results
 * together with comma.
 */
VALUE
pgconn_quote_all( argc, argv, self)
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
 *    conn.quote_identifier() -> str
 *
 * Put double quotes around an identifier containing non-letters
 * or upper case.
 */
VALUE
pgconn_quote_identifier( self, str)
    VALUE self, str;
{
    char *p;
    VALUE result;

    StringValue( str);
    p = PQescapeIdentifier( get_pgconn( self),
                    RSTRING_PTR( str), RSTRING_LEN( str));
    result = rb_str_new2( p);
    PQfreemem( p);
    OBJ_INFECT( result, str);
    return result;
}




/*
 * call-seq:
 *    conn.client_encoding() -> str
 *
 * Returns the client encoding as a String.
 */
VALUE
pgconn_client_encoding( self)
    VALUE self;
{
    char *encoding = (char *) pg_encoding_to_char(
                                PQclientEncoding( get_pgconn( self)));
    return rb_tainted_str_new2( encoding);
}

/*
 * call-seq:
 *    conn.set_client_encoding( encoding)
 *
 * Sets the client encoding to the +encoding+ string.
 */
VALUE
pgconn_set_client_encoding( self, str)
    VALUE self, str;
{
    StringValue( str);
    if ((PQsetClientEncoding( get_pgconn( self), RSTRING_PTR( str))) == -1)
        rb_raise( rb_ePgError, "Invalid encoding name %s", str);
    return Qnil;
}


char **
params_to_strings( conn, params)
    VALUE conn, params;
{
    VALUE *ptr;
    int len;
    char **values, **v;
    VALUE str;
    char *a;

    ptr = RARRAY_PTR( params);
    len = RARRAY_LEN( params);
    values = ALLOC_N( char *, len);
    for (v = values; len; v++, ptr++, len--)
        if (NIL_P( *ptr))
            *v = NULL;
        else {
            char *p, *q;

            str = pgconn_stringize( conn, *ptr);
            a = ALLOC_N( char, RSTRING_LEN( str) + 1);
            for (p = a, q = RSTRING_PTR( str); *p = *q; ++p, ++q)
                ;
            *v = a;
        }
    return values;
}

void
free_strings( strs, len)
    char **strs;
    int len;
{
    char **p;
    int l;

    for (p = strs, l = len; l; --l, ++p)
        xfree( *p);
    xfree( strs);
}


VALUE
exec_sql_statement( argc, argv, self, store)
    int argc;
    VALUE *argv;
    VALUE self;
    int store;
{
    PGconn *conn;
    PGresult *result;
    VALUE command, params;
    int len;

    conn = get_pgconn( self);
    rb_scan_args( argc, argv, "1*", &command, &params);
    StringValue( command);
    len = RARRAY_LEN( params);
    if (len <= 0)
        result = PQexec( conn, RSTRING_PTR( command));
    else {
        char **v;

        v = params_to_strings( self, params);
        result = PQexecParams( conn, RSTRING_PTR( command), len,
                               NULL, (const char **) v, NULL, NULL, 0);
        free_strings( v, len);
    }
    if (result == NULL)
        pg_raise_pgconn( conn);
    if (pg_checkresult( result) < 0)
        rb_exc_raise( pgreserror_new( result, command, params));
    if (store)
        pgconn_cmd_set( self, command, params);
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
    return yield_or_return_result( exec_sql_statement( argc, argv, self, 0));
}


VALUE
clear_resultqueue( self)
    VALUE self;
{
    PGconn *conn;
    PGresult *result;

    conn = get_pgconn( self);
    while ((result = PQgetResult( conn)) != NULL)
        PQclear( result);
    pgconn_cmd_clear( self);
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
    StringValue( command);
    len = RARRAY_LEN( params);
    conn = get_pgconn( self);
    if (len <= 0)
        res = PQsendQuery( conn, RSTRING_PTR( command));
    else {
        char **v;

        v = params_to_strings( self, params);
        res = PQsendQueryParams( conn, RSTRING_PTR( command), len,
                                 NULL, (const char **) v, NULL, NULL, 0);
        free_strings( v, len);
    }
    if (res <= 0)
        pg_raise_pgconn( conn);
    pgconn_cmd_set( self, command, params);
    return rb_ensure( rb_yield, Qnil, clear_resultqueue, self);
}

/*
 * call-seq:
 *    conn.fetch()                   -> result or nil
 *    conn.fetch() { |result| ... }  -> obj
 *
 * Fetches the results of the previous Pg::Conn#send call.
 * See there for an example.
 *
 * The result will be +nil+ if there are no more results.
 */
VALUE
pgconn_fetch( self)
    VALUE self;
{
    PGconn *conn;
    PGresult *result;
    VALUE res;

    conn = get_pgconn( self);
    if (PQconsumeInput( conn) == 0)
        pg_raise_pgconn( conn);
    if (PQisBusy( conn) > 0)
        return Qnil;
    result = PQgetResult( conn);
    if (result == NULL)
        res = Qnil;
    else {
        if (pg_checkresult( result) < 0)
            pg_raise_pgres( self, result);
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

    result = exec_sql_statement( argc, argv, self, 0);
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

    res = exec_sql_statement( argc, argv, self, 0);
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

    res = exec_sql_statement( argc, argv, self, 0);
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

    res = exec_sql_statement( argc, argv, self, 0);
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
        pg_raise_pgconn( conn);
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
rescue_transaction( self)
    VALUE self;
{
    pg_pqexec( get_pgconn( self), rb_str_new2( "rollback;"));
    rb_exc_raise( RB_ERRINFO);
    return Qnil;
}

VALUE
yield_transaction( self)
    VALUE self;
{
    VALUE r;

    r = rb_yield( self);
    pg_pqexec( get_pgconn( self), rb_str_new2( "commit;"));
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
    PGconn *conn;

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

    conn = get_pgconn( self);
    if (PQtransactionStatus( conn) > PQTRANS_IDLE)
        rb_raise( rb_ePgTransError,
            "Nested transaction block. Use Conn#subtransaction.");
    pg_pqexec( conn, cmd);
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
    pg_pqexec( get_pgconn( rb_ary_entry( ary, 0)), cmd);

    rb_exc_raise( RB_ERRINFO);
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
    pg_pqexec( get_pgconn( rb_ary_entry( ary, 0)), cmd);

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

    cmd = rb_str_buf_new2( "savepoint ");
    rb_str_buf_append( cmd, sp);
    rb_str_buf_cat2( cmd, ";");
    pg_pqexec( get_pgconn( self), cmd);

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
pgconn_transaction_status( self)
    VALUE self;
{
    return INT2NUM( PQtransactionStatus( get_pgconn( self)));
}






VALUE
put_end( self)
    VALUE self;
{
    PGconn *conn;
    int r;
    PGresult *res;

    conn = get_pgconn( self);
    /*
     * I would like to hand over something like
     *     RSTRING_PTR( rb_obj_as_string( RB_ERRINFO))
     * here but when execution is inside a rescue block
     * the error info will be non-null even though the
     * exception just has been caught.
     */
    while ((r = PQputCopyEnd( conn, NULL)) == 0)
        ;
    if (r < 0)
        pg_raise_pgconn( conn);

    while ((res = PQgetResult( conn)) != NULL) {
        if (pg_checkresult( res) < 0 && NIL_P( RB_ERRINFO))
            pg_raise_pgres( self, res);
        PQclear( res);
    }
    pgconn_cmd_clear( self);
    return Qnil;
}

/*
 * call-seq:
 *    conn.copy_stdin( sql, *bind_values) { |result| ... }   ->  nil
 *
 * Write lines into a +COPY+ command. See +stringize_line+ for how to build
 * standard lines.
 *
 *   conn.copy_stdin "copy t from stdin;" do
 *      ary = ...
 *      l = stringize_line ary
 *      conn.put l
 *   end
 *
 * You may write a "\\." yourself if you like it.
 */
VALUE
pgconn_copy_stdin( argc, argv, self)
    int argc;
    VALUE *argv;
    VALUE self;
{
    VALUE result;

    result = exec_sql_statement( argc, argv, self, 1);
    return rb_ensure( rb_yield, result, put_end, self);
}

/*
 * call-seq:
 *    conn.putline( str)         -> nil
 *    conn.putline( ary)         -> nil
 *    conn.putline( str) { ... } -> nil
 *
 * Sends the string to the backend server.
 * You have to open the stream with a +COPY+ command using +copy_stdin+.
 *
 * If +str+ doesn't end in a newline, one is appended. If the argument
 * is +ary+, a line will be built using +stringize_line+.
 *
 * If the connection is in nonblocking mode the block will be called
 * and its value will be returned.
 */
VALUE
pgconn_putline( self, arg)
    VALUE self, arg;
{
    VALUE str;
    char *p;
    int l;
    PGconn *conn;
    int r;

    switch (TYPE( arg)) {
    case T_STRING:
        str = arg;
        break;
    case T_ARRAY:
        str = pgconn_stringize_line( self, arg);
        break;
    default:
        str = rb_obj_as_string( arg);
        break;
    }
    p = RSTRING_PTR( str), l = RSTRING_LEN( str);
    if (p[ l - 1] != '\n') {
        VALUE t;

        t = rb_str_new( p, l);
        rb_str_buf_cat( t, "\n", 1);
        p = RSTRING_PTR( t), l = RSTRING_LEN( t);
    }

    conn = get_pgconn( self);
    r = PQputCopyData( conn, p, l);
    if (r < 0)
        pg_raise_pgconn( conn);
    else if (r == 0)
        return rb_yield( Qnil);
    return Qnil;
}


VALUE
get_end( self)
    VALUE self;
{
    PGconn *conn;
    char *b;
    PGresult *res;

    conn = get_pgconn( self);

    while ((res = PQgetResult( conn)) != NULL) {
        if (pg_checkresult( res) < 0 && NIL_P( RB_ERRINFO))
            pg_raise_pgres( self, res);
        PQclear( res);
    }
    pgconn_cmd_clear( self);
    return Qnil;
}

/*
 * call-seq:
 *    conn.copy_stdout( sql, *bind_values) { ... }   ->  nil
 *
 * Read lines from a +COPY+ command.  The form of the lines depends
 * on the statement's parameters.
 *
 *   conn.copy_stdout "copy t to stdout;" do
 *     l = conn.getline
 *     ary = l.split /\t/
 *     ary.map! { |x|
 *       unless x == "\\N" then
 *         x.gsub! /\\(.)/ do
 *           case $1
 *              when "t"  then "\t"
 *              when "n"  then "\n"
 *              when "\\" then "\\"
 *           end
 *         end
 *       end
 *     }
 *     ...
 *   end
 */
VALUE
pgconn_copy_stdout( argc, argv, self)
    int argc;
    VALUE *argv;
    VALUE self;
{
    VALUE result;

    result = exec_sql_statement( argc, argv, self, 1);
    return rb_ensure( rb_yield, result, get_end, self);
}

/*
 * call-seq:
 *    conn.getline( async = nil)         -> str
 *    conn.getline( async = nil) { ... } -> str
 *
 * Reads a line from the backend server after a +COPY+ command.
 * Returns +nil+ for EOF.
 *
 * If async is +true+ then the block will be called and its value
 * will be returned.
 *
 * Call this method inside a block passed to +copy_stdout+. See
 * there for an example.
 */
VALUE
pgconn_getline( argc, argv, self)
    int argc;
    VALUE *argv;
    VALUE self;
{
    VALUE as;
    int async;
    PGconn *conn;
    char *b;
    int r;

    async = rb_scan_args( argc, argv, "01", &as) > 0 && !NIL_P( as) ? 1 : 0;

    conn = get_pgconn( self);
    r = PQgetCopyData( conn, &b, async);
    if (r > 0) {
        VALUE ret;

        ret = rb_tainted_str_new( b, r);
        PQfreemem( b);
        return ret;
    } else if (r == 0)
        return rb_yield( Qnil);
    else {
        /* PQgetResult() will be called in the ensure block. */
    }
    return Qnil;
}

/*
 * call-seq:
 *    conn.each_line() { |line| ... } -> nil
 *
 * Reads line after line from a +COPY+ command.
 *
 * Call this method inside a block passed to +copy_stdout+. See
 * there for an example.
 */
VALUE
pgconn_each_line( self)
    VALUE self;
{
    PGconn *conn;
    char *b;
    int r;
    VALUE s;

    conn = get_pgconn( self);
    for (; (r = PQgetCopyData( conn, &b, 0)) > 0;) {
        s = rb_tainted_str_new( b, r);
        PQfreemem( b);
        rb_yield( s);
    }
    return Qnil;
}



void
pgconn_cmd_set( self, command, params)
    VALUE self;
    VALUE command;
    VALUE params;
{
    rb_ivar_set( self, id_command,    command);
    rb_ivar_set( self, id_parameters, params);
}

void
pgconn_cmd_clear( self)
    VALUE self;
{
    rb_ivar_set( self, id_command,    Qnil);
    rb_ivar_set( self, id_parameters, Qnil);
}


void
pg_raise_pgconn( conn)
    PGconn *conn;
{
    rb_raise( rb_ePgConnError, PQerrorMessage( conn));
}

void
pg_raise_pgres( self, result)
    VALUE self;
    PGresult *result;
{
    VALUE c, p;

    c = rb_ivar_get( self, id_command);
    p = rb_ivar_get( self, id_parameters);
    pgconn_cmd_clear( self);
    rb_exc_raise( pgreserror_new( result, c, p));
}


/********************************************************************
 *
 * Document-class: Pg::Conn
 *
 * The class to access a PostgreSQL database.
 *
 * For example, to send a query to the database on the localhost:
 *
 *    require "pgsql"
 *    conn = Pg::Conn.open :dbname => "test1"
 *    res = conn.exec "select * from mytable;"
 *
 * See the Pg::Result class for information on working with the results of a
 * query.
 */

/********************************************************************
 *
 * Document-class: Pg::Conn::Error
 *
 * Error while querying from a PostgreSQL connection.
 */

void
Init_pgsql_conn( void)
{
#if 0
    rb_mPg = rb_define_module( "Pg");
#endif

    rb_cPgConn = rb_define_class_under( rb_mPg, "Conn", rb_cObject);

    rb_define_alloc_func( rb_cPgConn, pgconn_alloc);
    rb_define_singleton_method( rb_cPgConn, "connect", pgconn_s_connect, -1);
    rb_define_alias( rb_singleton_class( rb_cPgConn), "open", "connect");

    rb_define_singleton_method( rb_cPgConn, "parse", pgconn_s_parse, 1);
    rb_define_singleton_method( rb_cPgConn, "translate_results=",
                                           pgconn_s_translate_results_set, 1);

#define CONN_DEF( c) rb_define_const( rb_cPgConn, #c, INT2FIX( CONNECTION_ ## c))
    CONN_DEF( OK);
    CONN_DEF( BAD);
#undef CONN_DEF

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

    rb_define_method( rb_cPgConn, "format",
                                            pgconn_format, 1);
    rb_define_method( rb_cPgConn, "escape_bytea",
                                            pgconn_escape_bytea, 1);
    rb_define_singleton_method( rb_cPgConn, "escape_bytea",
                                            pgconn_escape_bytea, 1);
    rb_define_method( rb_cPgConn, "unescape_bytea",
                                            pgconn_unescape_bytea, 1);
    rb_define_singleton_method( rb_cPgConn, "unescape_bytea",
                                            pgconn_unescape_bytea, 1);
    rb_define_method( rb_cPgConn, "stringize",
                                            pgconn_stringize, 1);
    rb_define_method( rb_cPgConn, "stringize_line", pgconn_stringize_line, 1);

    rb_define_method( rb_cPgConn, "quote_bytea",
                                            pgconn_quote_bytea, 1);
    rb_define_method( rb_cPgConn, "quote", pgconn_quote, 1);
    rb_define_method( rb_cPgConn, "quote_all", pgconn_quote_all, -1);
    rb_define_alias( rb_cPgConn, "q", "quote_all");

    rb_define_method( rb_cPgConn, "quote_identifier", pgconn_quote_identifier, 1);
    rb_define_alias( rb_cPgConn, "quote_ident", "quote_identifier");

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

#define TRANS_DEF( c) rb_define_const( rb_cPgConn, "T_" #c, INT2FIX( PQTRANS_ ## c))
    TRANS_DEF( IDLE);
    TRANS_DEF( ACTIVE);
    TRANS_DEF( INTRANS);
    TRANS_DEF( INERROR);
    TRANS_DEF( UNKNOWN);
#undef TRANS_DEF
    rb_define_method( rb_cPgConn, "transaction", pgconn_transaction, -1);
    rb_define_method( rb_cPgConn, "subtransaction", pgconn_subtransaction, -1);
    rb_define_alias( rb_cPgConn, "savepoint", "subtransaction");
    rb_define_method( rb_cPgConn, "transaction_status",
                                                 pgconn_transaction_status, 0);

    rb_define_method( rb_cPgConn, "copy_stdin", pgconn_copy_stdin, -1);
    rb_define_method( rb_cPgConn, "putline", pgconn_putline, 1);
    rb_define_alias( rb_cPgConn, "put", "putline");
    rb_define_method( rb_cPgConn, "copy_stdout", pgconn_copy_stdout, -1);
    rb_define_method( rb_cPgConn, "getline", pgconn_getline, -1);
    rb_define_alias( rb_cPgConn, "get", "getline");
    rb_define_method( rb_cPgConn, "each_line", pgconn_each_line, 0);
    rb_define_alias( rb_cPgConn, "eat_lines", "each_line");

    rb_ePgConnectFailed = rb_define_class_under( rb_mPg, "ConnectFailed", rb_ePgError);
#define ERR_DEF( n)  rb_define_class_under( rb_cPgConn, n, rb_ePgError)
    rb_ePgConnError  = ERR_DEF( "Error");
    rb_ePgTransError = ERR_DEF( "InTransaction");
#undef ERR_DEF

    id_to_postgres = rb_intern( "to_postgres");
    id_raw         = rb_intern( "raw");
    id_format      = rb_intern( "format");
    id_iso8601     = rb_intern( "iso8601");
    id_rows        = 0;
    id_call        = 0;
    id_on_notice   = 0;
    id_gsub_bang   = rb_intern( "gsub!");
    id_command     = rb_intern( "command");
    id_parameters  = rb_intern( "parameters");

    pg_escape_regex = Qnil;
}

