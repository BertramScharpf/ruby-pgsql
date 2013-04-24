/*
 *  conn.c  --  PostgreSQL connection
 */


#include "conn.h"

#if defined( HAVE_HEADER_ST_H)
    #include <st.h>
#endif


#ifdef TODO_DONE
#include "result.h"

#ifdef HAVE_FUNC_RB_ERRINFO
    #define RB_ERRINFO (rb_errinfo())
#else
    #define RB_ERRINFO ruby_errinfo
#endif

#ifdef HAVE_HEADER_LIBPQ_LIBPQ_FS_H
    #include <libpq/libpq-fs.h>
#endif

#endif

extern void pg_check_conninvalid( struct pgconn_data *c);


static void  pgconn_mark( struct pgconn_data *ptr);
static void  pgconn_free( struct pgconn_data *ptr);
extern void  pgconn_clear( struct pgconn_data *c);
extern struct pgconn_data *get_pgconn( VALUE obj);
static void  pgconn_encode_out( struct pgconn_data *ptr, VALUE str);
extern VALUE pgconn_mkstring( struct pgconn_data *ptr, const char *str);
extern VALUE pgconn_mkstringn( struct pgconn_data *ptr, const char *str, int len);
static VALUE pgconn_alloc( VALUE cls);
static VALUE pgconn_s_connect( int argc, VALUE *argv, VALUE cls);
static VALUE pgconn_s_parse( VALUE cls, VALUE str);
static VALUE pgconn_init( int argc, VALUE *argv, VALUE self);
static int   set_connect_params( st_data_t key, st_data_t val, st_data_t args);
static void  connstr_to_hash( VALUE params, VALUE str);
static void  connstr_passwd( VALUE self, VALUE params);
static VALUE pgconn_close( VALUE obj);
static VALUE pgconn_reset( VALUE obj);

static VALUE pgconn_client_encoding( VALUE obj);
static VALUE pgconn_set_client_encoding( VALUE obj, VALUE str);

static VALUE pgconn_protocol_version( VALUE obj);
static VALUE pgconn_server_version(   VALUE obj);

static VALUE pgconn_db(      VALUE obj);
static VALUE pgconn_host(    VALUE obj);
static VALUE pgconn_options( VALUE obj);
static VALUE pgconn_port(    VALUE obj);
static VALUE pgconn_tty(     VALUE obj);
static VALUE pgconn_user(    VALUE obj);
static VALUE pgconn_status(  VALUE obj);
static VALUE pgconn_error(   VALUE obj);

static VALUE pgconn_socket(  VALUE obj);

static VALUE pgconn_trace(   VALUE obj, VALUE port);
static VALUE pgconn_untrace( VALUE obj);


VALUE rb_cPgConn;

static VALUE rb_ePgConnFailed;
static VALUE rb_ePgConnInvalid;


void
pg_check_conninvalid( struct pgconn_data *c)
{
    if (c->conn == NULL)
        rb_raise( rb_ePgConnInvalid, "Invalid connection (probably closed).");
}



void
pgconn_mark( struct pgconn_data *ptr)
{
    rb_gc_mark( ptr->command);
    rb_gc_mark( ptr->params);
    rb_gc_mark( ptr->notice);
}

void
pgconn_free( struct pgconn_data *ptr)
{
    if (ptr->conn != NULL)
        PQfinish( ptr->conn);
    free( ptr);
}

void
pgconn_clear( struct pgconn_data *c)
{
    c->command = Qnil;
    c->params  = Qnil;
}

struct pgconn_data *
get_pgconn( VALUE obj)
{
    struct pgconn_data *c;

    Data_Get_Struct( obj, struct pgconn_data, c);
    pg_check_conninvalid( c);
    return c;
}

void
pgconn_encode_out( struct pgconn_data *ptr, VALUE str)
{
#ifdef TODO_RUBY19_ENCODING
    rb_encode_force_to( str, ptr->external);
    if (ptr->internal != NULL)
        rb_encode_change_to( str, ptr->internal);
#endif
}


VALUE
pgconn_mkstring( struct pgconn_data *ptr, const char *str)
{
    VALUE r;

    if (str) {
        r = rb_tainted_str_new2( str);
        pgconn_encode_out( ptr, r);
    } else
        r = Qnil;
    return r;
}

VALUE
pgconn_mkstringn( struct pgconn_data *ptr, const char *str, int len)
{
    VALUE r;

    if (str) {
        r = rb_tainted_str_new( str, len);
        pgconn_encode_out( ptr, r);
    } else
        r = Qnil;
    return r;
}

VALUE
pgconn_alloc( VALUE cls)
{
    struct pgconn_data *c;
    VALUE r;

    r = Data_Make_Struct( cls, struct pgconn_data,
                            &pgconn_mark, &pgconn_free, c);
    c->conn    = NULL;
    c->command = Qnil;
    c->params  = Qnil;
    c->notice  = Qnil;
    return r;
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
pgconn_s_connect( int argc, VALUE *argv, VALUE cls)
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
pgconn_s_parse( VALUE cls, VALUE str)
{
    VALUE params;

    params = rb_hash_new();
    connstr_to_hash( params, str);
    return params;
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
pgconn_init( int argc, VALUE *argv, VALUE self)
{
    VALUE str, params;
    int l;
    const char **keywords, **values;
    const char **ptrs[ 2];
    struct pgconn_data *c;

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
        connstr_to_hash( params, str);
    connstr_passwd( self, params);

    l = RHASH_SIZE( params) + 1;
    keywords = (const char **) ALLOCA_N( char *, l);
    values   = (const char **) ALLOCA_N( char *, l);
    ptrs[ 0] = keywords;
    ptrs[ 1] = values;
    st_foreach( RHASH_TBL( params), set_connect_params, (st_data_t) ptrs);
    *(ptrs[ 0]) = *(ptrs[ 1]) = NULL;

    Data_Get_Struct( self, struct pgconn_data, c);
    c->conn = PQconnectdbParams( keywords, values, 0);
    if (PQstatus( c->conn) == CONNECTION_BAD)
        rb_raise( rb_ePgConnFailed, PQerrorMessage( c->conn));

#ifdef TODO_RUBY19_ENCODING
    c->external = rb_default_external_encoding();
    c->internal = rb_default_internal_encoding();
#endif

    return self;
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

/*
 * call-seq:
 *    conn.close()
 *
 * Closes the backend connection.
 */
VALUE
pgconn_close( VALUE obj)
{
    struct pgconn_data *c;

    Data_Get_Struct( obj, struct pgconn_data, c);
    PQfinish( c->conn);
    c->conn = NULL;
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
pgconn_reset( VALUE obj)
{
    PQreset( get_pgconn( obj)->conn);
    return obj;
}



/*
 * call-seq:
 *    conn.client_encoding() -> str
 *
 * Returns the client encoding as a String.
 */
VALUE
pgconn_client_encoding( VALUE self)
{
    char *encoding = (char *) pg_encoding_to_char(
                            PQclientEncoding( get_pgconn( self)->conn));
    return rb_tainted_str_new2( encoding);
}

/*
 * call-seq:
 *    conn.set_client_encoding( encoding)
 *
 * Sets the client encoding to the +encoding+ string.
 */
VALUE
pgconn_set_client_encoding( VALUE self, VALUE str)
{
    StringValue( str);
    if ((PQsetClientEncoding( get_pgconn( self)->conn, RSTRING_PTR( str))) == -1)
        rb_raise( rb_ePgError, "Invalid encoding name %s", str);
    return Qnil;
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
pgconn_protocol_version( VALUE obj)
{
    return INT2FIX( PQprotocolVersion( get_pgconn( obj)->conn));
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
pgconn_server_version( VALUE obj)
{
    return INT2FIX( PQserverVersion( get_pgconn( obj)->conn));
}


/*
 * call-seq:
 *    conn.db()
 *
 * Returns the connected database name.
 */
VALUE
pgconn_db( VALUE obj)
{
    struct pgconn_data *c;
    char *db;

    c = get_pgconn( obj);
    db = PQdb( c->conn);
    return db == NULL ? Qnil : pgconn_mkstring( c, db);
}

/*
 * call-seq:
 *    conn.host()
 *
 * Returns the connected server name.
 */
VALUE
pgconn_host( VALUE obj)
{
    struct pgconn_data *c;
    char *host;

    c = get_pgconn( obj);
    host = PQhost( c->conn);
    return host == NULL ? Qnil : pgconn_mkstring( c, host);
}

/*
 * call-seq:
 *    conn.options()
 *
 * Returns backend option string.
 */
VALUE
pgconn_options( VALUE obj)
{
    struct pgconn_data *c;
    char *options;

    c = get_pgconn( obj);
    options = PQoptions( c->conn);
    return options == NULL ? Qnil : pgconn_mkstring( c, options);
}

/*
 * call-seq:
 *    conn.port()
 *
 * Returns the connected server port number.
 */
VALUE
pgconn_port( VALUE obj)
{
    char* port = PQport( get_pgconn( obj)->conn);
    return port == NULL ? Qnil : INT2FIX( atol( port));
}

/*
 * call-seq:
 *    conn.tty()
 *
 * Returns the connected pgtty.
 */
VALUE
pgconn_tty( VALUE obj)
{
    struct pgconn_data *c;
    char *tty;

    c = get_pgconn( obj);
    tty = PQtty( c->conn);
    return tty == NULL ? Qnil : pgconn_mkstring( c, tty);
}

/*
 * call-seq:
 *    conn.user()
 *
 * Returns the authenticated user name.
 */
VALUE
pgconn_user( VALUE obj)
{
    struct pgconn_data *c;
    char *user;

    c = get_pgconn( obj);
    user = PQuser( c->conn);
    return user == NULL ? Qnil : pgconn_mkstring( c, user);
}

/*
 * call-seq:
 *    conn.status()
 *
 * This may return the values +CONNECTION_OK+ or +CONNECTION_BAD+.
 */
VALUE
pgconn_status( VALUE obj)
{
    return INT2FIX( PQstatus( get_pgconn( obj)->conn));
}

/*
 * call-seq:
 *    conn.error()
 *
 * Returns the error message about connection.
 */
VALUE
pgconn_error( VALUE obj)
{
    struct pgconn_data *c;
    char *error;

    c = get_pgconn( obj);
    error = PQerrorMessage( c->conn);
    return error == NULL ? Qnil : pgconn_mkstring( c, error);
}



/*
 * call-seq:
 *    conn.socket()  -> io
 *
 * Returns the sockets IO object.
 */
VALUE
pgconn_socket( VALUE obj)
{
    static ID id_new = 0;
    int fd;

    if (id_new == 0)
        id_new = rb_intern( "new");

    fd = PQsocket( get_pgconn( obj)->conn);
    return rb_funcall( rb_cIO, id_new, 1, INT2FIX( fd));
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
pgconn_trace( VALUE self, VALUE port)
{
#ifdef HAVE_FUNC_RB_IO_STDIO_FILE
    rb_io_t *fp;
#else
    OpenFile* fp;
    #define rb_io_stdio_file GetWriteFile
#endif

    if (TYPE( port) != T_FILE)
        rb_raise( rb_eArgError, "Not an IO object: %s",
                                StringValueCStr( port));
    GetOpenFile( port, fp);
    PQtrace( get_pgconn( self)->conn, rb_io_stdio_file( fp));
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
pgconn_untrace( VALUE self)
{
    PQuntrace( get_pgconn( self)->conn);
    return Qnil;
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

void
Init_pgsql_conn( void)
{
    rb_cPgConn = rb_define_class_under( rb_mPg, "Conn", rb_cObject);

#define ERR_DEF( n)  rb_define_class_under( rb_cPgConn, n, rb_ePgError)
    rb_ePgConnFailed  = ERR_DEF( "Failed");
    rb_ePgConnInvalid = ERR_DEF( "Invalid");
#undef ERR_DEF

    rb_define_alloc_func( rb_cPgConn, pgconn_alloc);
    rb_define_singleton_method( rb_cPgConn, "connect", pgconn_s_connect, -1);
    rb_define_alias( rb_singleton_class( rb_cPgConn), "open", "connect");
    rb_define_singleton_method( rb_cPgConn, "parse", pgconn_s_parse, 1);
    rb_define_method( rb_cPgConn, "initialize", pgconn_init, -1);
    rb_define_method( rb_cPgConn, "close", pgconn_close, 0);
    rb_define_alias( rb_cPgConn, "finish", "close");
    rb_define_method( rb_cPgConn, "reset", pgconn_reset, 0);

    rb_define_method( rb_cPgConn, "client_encoding", pgconn_client_encoding, 0);
    rb_define_method( rb_cPgConn, "set_client_encoding", pgconn_set_client_encoding, 1);
#ifdef TODO_RUBY19_ENCODING
    rb_define_method( rb_cPgConn, "external_encoding",  pgconn_externalenc, 0);
    rb_define_method( rb_cPgConn, "external_encoding=", pgconn_set_externalenc, 0);
    rb_define_method( rb_cPgConn, "internal_encoding",  pgconn_internalenc, 0);
    rb_define_method( rb_cPgConn, "internal_encoding=", pgconn_set_internalenc, 0);
#endif

    rb_define_method( rb_cPgConn, "protocol_version", pgconn_protocol_version, 0);
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

#define CONN_DEF( c) \
            rb_define_const( rb_cPgConn, #c, INT2FIX( CONNECTION_ ## c))
    CONN_DEF( OK);
    CONN_DEF( BAD);
#undef CONN_DEF

    rb_define_method( rb_cPgConn, "socket", pgconn_socket, 0);

    rb_define_method( rb_cPgConn, "trace", pgconn_trace, 1);
    rb_define_method( rb_cPgConn, "untrace", pgconn_untrace, 0);

    Init_pgsql_conn_quote();
    Init_pgsql_conn_exec();

#ifdef TODO_DONE

    rb_define_method( rb_cPgConn, "on_notice", pgconn_on_notice, 0);

#define TRANS_DEF( c) \
            rb_define_const( rb_cPgConn, "T_" #c, INT2FIX( PQTRANS_ ## c))
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

#define ERR_DEF( n)  rb_define_class_under( rb_cPgConn, n, rb_ePgError)
    rb_ePgTransError = ERR_DEF( "InTransaction");
#undef ERR_DEF

    id_on_notice   = 0;

#endif
}


#ifdef TODO_DONE

static ID id_on_notice;



static void  notice_receiver( void *self, const PGresult *result);
static VALUE pgconn_on_notice( VALUE self);



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


static VALUE rb_ePgTransError;



PGconn *
get_pgconn( VALUE obj)
{
    struct pgconn_data *c;

    Data_Get_Struct( obj, struct pgconn_data, c);
    if (c->conn == NULL)
        rb_raise( rb_ePgError, "not a valid connection");
    return c->conn;
}

void
notice_receiver( void *self, const PGresult *result)
{
    struct pgconn_data *c;
    VALUE block;

    Data_Get_Struct( obj, struct pgconn_data, c);
    if (c->notice != Qnil) {
        VALUE err;

        err = pgreserror_new( (PGresult *) result, c);
        rb_proc_call( c->notice, rb_ary_new4( 1l, &err));
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
pgconn_on_notice( VALUE self)
{
    PGconn *conn;

    conn = get_pgconn( self);
    if (PQsetNoticeReceiver( conn, NULL, NULL) != &notice_receiver)
        PQsetNoticeReceiver( conn, &notice_receiver, (void *) self);
    if (!id_on_notice)
        id_on_notice = rb_intern( "@on_notice");
    rb_ivar_set( self, id_on_notice, rb_block_proc());
    return self;
}








VALUE
rescue_transaction( VALUE self)
{
    pg_pqexec( get_pgconn( self), rb_str_new2( "rollback;"));
    rb_exc_raise( RB_ERRINFO);
    return Qnil;
}

VALUE
yield_transaction( VALUE self)
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
pgconn_transaction( int argc, VALUE *argv, VALUE self)
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
        rb_str_buf_cat2( cmd, RTEST(ser) ? "serializable" : "read committed");
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
rescue_subtransaction( VALUE ary)
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
yield_subtransaction( VALUE ary)
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
pgconn_subtransaction( int argc, VALUE *argv, VALUE self)
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
pgconn_transaction_status( VALUE self)
{
    return INT2FIX( PQtransactionStatus( get_pgconn( self)));
}






VALUE
put_end( VALUE self)
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
        pg_checkresult( result, c);
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
pgconn_copy_stdin( int argc, VALUE *argv, VALUE self)
{
    struct pgconn_data *c;
    VALUE res;

    Data_Get_Struct( self, struct pgconn_data, c);
    res = pgresult_new( c, exec_sql_statement( argc, argv, self, 0));
    return rb_ensure( rb_yield, res, put_end, self);
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
pgconn_putline( VALUE self, VALUE arg)
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
get_end( VALUE self)
{
    PGconn *conn;
    char *b;
    PGresult *res;

    conn = get_pgconn( self);

    while ((res = PQgetResult( conn)) != NULL) {
        if (NIL_P( RB_ERRINFO))
            pg_checkresult( result, c);
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
pgconn_copy_stdout( int argc, VALUE *argv, VALUE self)
{
    struct pgconn_data *c;
    VALUE res;

    Data_Get_Struct( self, struct pgconn_data, c);
    res = pgresult_new( c, exec_sql_statement( argc, argv, self, 0));
    return rb_ensure( rb_yield, res, get_end, self);
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
pgconn_getline( int argc, VALUE *argv, VALUE self)
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
pgconn_each_line( VALUE self)
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




/********************************************************************
 *
 * Document-class: Pg::Conn::Error
 *
 * Error while querying from a PostgreSQL connection.
 */

#endif

