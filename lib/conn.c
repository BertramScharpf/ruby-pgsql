/*
 *  conn.c  --  PostgreSQL connection
 */


#include "conn.h"



extern void pg_check_conninvalid( struct pgconn_data *c);


static void  pgconn_mark( struct pgconn_data *ptr);
static void  pgconn_free( struct pgconn_data *ptr);
extern void  pgconn_clear( struct pgconn_data *c);
extern struct pgconn_data *get_pgconn( VALUE obj);
static VALUE pgconn_encode_in4out( struct pgconn_data *ptr, VALUE str);
extern const char *pgconn_destring( struct pgconn_data *ptr, VALUE str, int *len);
static void  pgconn_encode_out4in( struct pgconn_data *ptr, VALUE str);
extern VALUE pgconn_mkstring( struct pgconn_data *ptr, const char *str);
extern VALUE pgconn_mkstringn( struct pgconn_data *ptr, const char *str, int len);
static VALUE pgconn_alloc( VALUE cls);
static VALUE pgconn_s_connect( int argc, VALUE *argv, VALUE cls);
static VALUE pgconn_s_parse( VALUE cls, VALUE str);
static VALUE pgconn_init( int argc, VALUE *argv, VALUE self);
static int   set_connect_params( st_data_t key, st_data_t val, st_data_t args);
static void  connstr_to_hash( VALUE params, VALUE str);
static void  connstr_passwd( VALUE self, VALUE params);
static VALUE pgconn_close( VALUE self);
static VALUE pgconn_reset( VALUE self);

static VALUE pgconn_client_encoding( VALUE self);
static VALUE pgconn_set_client_encoding( VALUE self, VALUE str);
#ifdef RUBY_ENCODING
static VALUE pgconn_externalenc( VALUE self);
static VALUE pgconn_set_externalenc( VALUE self, VALUE enc);
static VALUE pgconn_internalenc( VALUE self);
static VALUE pgconn_set_internalenc( VALUE self, VALUE enc);
#endif

static VALUE pgconn_protocol_version( VALUE self);
static VALUE pgconn_server_version(   VALUE self);

static VALUE pgconn_db(      VALUE self);
static VALUE pgconn_host(    VALUE self);
static VALUE pgconn_options( VALUE self);
static VALUE pgconn_port(    VALUE self);
static VALUE pgconn_tty(     VALUE self);
static VALUE pgconn_user(    VALUE self);
static VALUE pgconn_status(  VALUE self);
static VALUE pgconn_error(   VALUE self);

static VALUE pgconn_socket(  VALUE self);

static VALUE pgconn_trace( int argc, VALUE *argv, VALUE self);
static VALUE pgconn_untrace( VALUE self);

static VALUE pgconn_on_notice( VALUE self);
static void  notice_receiver( void *self, const PGresult *result);


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


VALUE pgconn_encode_in4out( struct pgconn_data *ptr, VALUE str)
{
    VALUE r;

    r = rb_obj_as_string( str);
#ifdef RUBY_ENCODING
    if (rb_enc_get( r) != ptr->external) {
        r = rb_str_dup( r);
        rb_enc_associate( r, ptr->external);
        // TODO: This is not working.
    }
#endif
    return r;
}

VALUE pgconn_debug( VALUE self, VALUE str)
{
    struct pgconn_data *c;

    Data_Get_Struct( self, struct pgconn_data, c);
    return pgconn_encode_in4out( c, str);
}


const char *pgconn_destring( struct pgconn_data *ptr, VALUE str, int *len)
{
    VALUE s;

    s = pgconn_encode_in4out( ptr, str);
    if (len != NULL)
        *len = RSTRING_LEN( s);
    return RSTRING_PTR( s);
}


void
pgconn_encode_out4in( struct pgconn_data *ptr, VALUE str)
{
#ifdef RUBY_ENCODING
    ENCODING_SET( str, rb_enc_to_index( ptr->external));
    if (ptr->internal != NULL)
        rb_enc_associate( str, ptr->internal);
#endif
}


VALUE
pgconn_mkstring( struct pgconn_data *ptr, const char *str)
{
    VALUE r;

    if (str) {
        r = rb_tainted_str_new2( str);
        pgconn_encode_out4in( ptr, r);
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
        pgconn_encode_out4in( ptr, r);
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
#ifdef RUBY_ENCODING
    c->external = rb_default_external_encoding();
    c->internal = rb_default_internal_encoding();
#endif
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
    const char **ptrs[ 3];
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
    ptrs[ 2] = (const char **) c;
    st_foreach( RHASH_TBL( params), &set_connect_params, (st_data_t) ptrs);
    *(ptrs[ 0]) = *(ptrs[ 1]) = NULL;

    Data_Get_Struct( self, struct pgconn_data, c);
    c->conn = PQconnectdbParams( keywords, values, 1);
    if (PQstatus( c->conn) == CONNECTION_BAD)
        rb_raise( rb_ePgConnFailed, PQerrorMessage( c->conn));

    return self;
}

int
set_connect_params( st_data_t key, st_data_t val, st_data_t args)
{
    const char ***ptrs = (const char ***)args;
    struct pgconn_data *c;
    VALUE k, v;

    k = (VALUE) key;
    v = (VALUE) val;
    c = (struct pgconn_data *) ptrs[ 2];
    if (!NIL_P( v)) {
        *(ptrs[ 0]) = pgconn_destring( c, rb_obj_as_string( k), NULL);
        *(ptrs[ 1]) = pgconn_destring( c, rb_obj_as_string( v), NULL);
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
pgconn_close( VALUE self)
{
    struct pgconn_data *c;

    Data_Get_Struct( self, struct pgconn_data, c);
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
pgconn_reset( VALUE self)
{
    PQreset( get_pgconn( self)->conn);
    return self;
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
 *    conn.set_client_encoding( encoding)   ->  nil
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


#ifdef RUBY_ENCODING

/*
 * call-seq:
 *    conn.external_encoding   ->  enc
 *
 * Return the external encoding.
 */
VALUE
pgconn_externalenc( VALUE self)
{
    struct pgconn_data *c;

    Data_Get_Struct( self, struct pgconn_data, c);
    return rb_enc_from_encoding( c->external);
}

/*
 * call-seq:
 *    conn.external_encoding = enc
 *
 * Set the external encoding.
 */
VALUE
pgconn_set_externalenc( VALUE self, VALUE enc)
{
    struct pgconn_data *c;

    Data_Get_Struct( self, struct pgconn_data, c);
    c->external = NIL_P( enc) ? NULL : rb_to_encoding( enc);
}

/*
 * call-seq:
 *    conn.internal_encoding   ->  enc
 *
 * Return the internal encoding.
 */
VALUE
pgconn_internalenc( VALUE self)
{
    struct pgconn_data *c;

    Data_Get_Struct( self, struct pgconn_data, c);
    return rb_enc_from_encoding( c->internal);
}

/*
 * call-seq:
 *    conn.internal_encoding = enc
 *
 * Set the internal encoding.
 */
VALUE
pgconn_set_internalenc( VALUE self, VALUE enc)
{
    struct pgconn_data *c;

    Data_Get_Struct( self, struct pgconn_data, c);
    c->internal = NIL_P( enc) ? NULL : rb_to_encoding( enc);
}

#endif



/*
 * call-seq:
 *  conn.protocol_version -> Integer
 *
 * The 3.0 protocol will normally be used when communicating with PostgreSQL
 * 7.4 or later servers; pre-7.4 servers support only protocol 2.0. (Protocol
 * 1.0 is obsolete and not supported by libpq.)
 */
VALUE
pgconn_protocol_version( VALUE self)
{
    return INT2FIX( PQprotocolVersion( get_pgconn( self)->conn));
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
pgconn_server_version( VALUE self)
{
    return INT2FIX( PQserverVersion( get_pgconn( self)->conn));
}


/*
 * call-seq:
 *    conn.db()
 *
 * Returns the connected database name.
 */
VALUE
pgconn_db( VALUE self)
{
    struct pgconn_data *c;
    char *db;

    c = get_pgconn( self);
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
pgconn_host( VALUE self)
{
    struct pgconn_data *c;
    char *host;

    c = get_pgconn( self);
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
pgconn_options( VALUE self)
{
    struct pgconn_data *c;
    char *options;

    c = get_pgconn( self);
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
pgconn_port( VALUE self)
{
    char* port = PQport( get_pgconn( self)->conn);
    return port == NULL ? Qnil : INT2FIX( atol( port));
}

/*
 * call-seq:
 *    conn.tty()
 *
 * Returns the connected pgtty.
 */
VALUE
pgconn_tty( VALUE self)
{
    struct pgconn_data *c;
    char *tty;

    c = get_pgconn( self);
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
pgconn_user( VALUE self)
{
    struct pgconn_data *c;
    char *user;

    c = get_pgconn( self);
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
pgconn_status( VALUE self)
{
    return INT2FIX( PQstatus( get_pgconn( self)->conn));
}

/*
 * call-seq:
 *    conn.error()
 *
 * Returns the error message about connection.
 */
VALUE
pgconn_error( VALUE self)
{
    struct pgconn_data *c;
    char *error;

    c = get_pgconn( self);
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
pgconn_socket( VALUE self)
{
    static ID id_new = 0;
    int fd;

    if (id_new == 0)
        id_new = rb_intern( "new");

    fd = PQsocket( get_pgconn( self)->conn);
    return rb_funcall( rb_cIO, id_new, 1, INT2FIX( fd));
}


/*
 * call-seq:
 *    conn.trace( file)
 *    conn.trace( file) { ... }
 *
 * Enables tracing message passing between backend.
 * The trace message will be written to the _file_ object,
 * which is an instance of the class +File+ (or at least +IO+).
 *
 * In case a block is given +untrace+ will be called automatically.
 */
VALUE
pgconn_trace( int argc, VALUE *argv, VALUE self)
{
    VALUE file;
#ifdef HAVE_FUNC_RB_IO_STDIO_FILE
    rb_io_t *fp;
#else
    OpenFile* fp;
    #define rb_io_stdio_file GetWriteFile
#endif

    if (rb_scan_args( argc, argv, "01", &file) > 0) {
        if (TYPE( file) != T_FILE)
            rb_raise( rb_eArgError, "Not an IO object: %s",
                                    StringValueCStr( file));
    } else
        file = rb_stdout;

    GetOpenFile( file, fp);
    PQtrace( get_pgconn( self)->conn, rb_io_stdio_file( fp));
    return rb_block_given_p() ?
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
    struct pgconn_data *c;

    Data_Get_Struct( self, struct pgconn_data, c);
    if (PQsetNoticeReceiver( c->conn, NULL, NULL) != &notice_receiver) {
        PQsetNoticeReceiver( c->conn, &notice_receiver, (void *) c);
    }
    c->notice = rb_block_given_p() ? rb_block_proc() : Qnil;
    return self;
}

void
notice_receiver( void *self, const PGresult *result)
{
    struct pgconn_data *c;

    c = (struct pgconn_data *) self;
    if (c->notice != Qnil) {
        VALUE err;

#if 0
/* This crashes; maybe because PQclear will also be done by Postgres. */
        err = pgreserror_new( (PGresult *) result, c);
#else
        err = pgconn_mkstring( c, PQresultErrorMessage( result));
#endif
        rb_proc_call( c->notice, rb_ary_new3( 1l, err));
    }
}




/********************************************************************
 *
 * Document-class: Pg::Conn::Failed
 *
 * Error while establishing a connection to the PostgreSQL server.
 */

/********************************************************************
 *
 * Document-class: Pg::Conn::Invalid
 *
 * Invalid (closed) connection.
 */


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
#ifdef RUBY_ENCODING
    rb_define_method( rb_cPgConn, "external_encoding",  pgconn_externalenc, 0);
    rb_define_method( rb_cPgConn, "external_encoding=", pgconn_set_externalenc, 1);
    rb_define_method( rb_cPgConn, "internal_encoding",  pgconn_internalenc, 0);
    rb_define_method( rb_cPgConn, "internal_encoding=", pgconn_set_internalenc, 1);
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

#define CONN_DEF( c) rb_define_const( rb_cPgConn, #c, INT2FIX( CONNECTION_ ## c))
    CONN_DEF( OK);
    CONN_DEF( BAD);
#undef CONN_DEF

    rb_define_method( rb_cPgConn, "socket", pgconn_socket, 0);

    rb_define_method( rb_cPgConn, "trace", pgconn_trace, -1);
    rb_define_method( rb_cPgConn, "untrace", pgconn_untrace, 0);

    rb_define_method( rb_cPgConn, "on_notice", pgconn_on_notice, 0);

    rb_define_method( rb_cPgConn, "xxx", pgconn_debug, 1);

    Init_pgsql_conn_quote();
    Init_pgsql_conn_exec();
}

