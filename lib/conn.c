/*
 *  conn.c  --  PostgreSQL connection
 */


#include "conn.h"

#include "conn_quote.h"
#include "conn_exec.h"

#if defined( HAVE_HEADER_ST_H)
    #include <st.h>
#endif


extern void pg_check_conninvalid( struct pgconn_data *c);
static VALUE pgconnfailederror_new( struct pgconn_data *c, VALUE params);

static void  pgconn_mark( struct pgconn_data *ptr);
static void  pgconn_free( struct pgconn_data *ptr);
extern struct pgconn_data *get_pgconn( VALUE obj);
static VALUE pgconn_encode_in4out( struct pgconn_data *ptr, VALUE str);
extern const char *pgconn_destring( struct pgconn_data *ptr, VALUE str, int *len);
static VALUE pgconn_encode_out4in( struct pgconn_data *ptr, VALUE str);
extern VALUE pgconn_mkstring( struct pgconn_data *ptr, const char *str);
extern VALUE pgconn_mkstringn( struct pgconn_data *ptr, const char *str, int len);
static VALUE pgconn_alloc( VALUE cls);
static VALUE pgconn_s_connect( int argc, VALUE *argv, VALUE cls);
static VALUE pgconn_init( int argc, VALUE *argv, VALUE self);
static int   set_connect_params( st_data_t key, st_data_t val, st_data_t args);
static VALUE connstr_sym_dbname( void);
static VALUE connstr_sym_password( void);
static void  connstr_passwd( VALUE self, VALUE orig_params, VALUE *params);
static VALUE connstr_getparam( RB_BLOCK_CALL_FUNC_ARGLIST( yielded, params));

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

static VALUE pgconn_dbname(  VALUE self);
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

static VALUE sym_dbname   = Qundef;
static VALUE sym_password = Qundef;



void
pg_check_conninvalid( struct pgconn_data *c)
{
    if (c->conn == NULL)
        rb_raise( rb_ePgConnInvalid, "Invalid connection (probably closed).");
}


VALUE
pgconnfailederror_new( struct pgconn_data *c, VALUE params)
{
    VALUE msg, cfe;

    msg = pgconn_mkstring( c, PQerrorMessage( c->conn));
    cfe = rb_class_new_instance( 1, &msg, rb_ePgConnFailed);
    rb_ivar_set( cfe, rb_intern( "@parameters"), params);
    return cfe;
}



void
pgconn_mark( struct pgconn_data *ptr)
{
    rb_gc_mark( ptr->notice);
#ifdef RUBY_ENCODING
    rb_gc_mark( ptr->external);
    rb_gc_mark( ptr->internal);
#endif
}

void
pgconn_free( struct pgconn_data *ptr)
{
    if (ptr->conn != NULL)
        PQfinish( ptr->conn);
    free( ptr);
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
    str = rb_obj_as_string( str);
#ifdef RUBY_ENCODING
    if (rb_enc_compatible( str, ptr->external) == NULL)
        str = rb_str_conv_enc( str, rb_enc_get( str), rb_to_encoding( ptr->external));
#endif
    return str;
}

const char *pgconn_destring( struct pgconn_data *ptr, VALUE str, int *len)
{
    VALUE s;

    s = pgconn_encode_in4out( ptr, str);
    if (len != NULL)
        *len = RSTRING_LEN( s);
    return RSTRING_PTR( s);
}


VALUE
pgconn_encode_out4in( struct pgconn_data *ptr, VALUE str)
{
#ifdef RUBY_ENCODING
    rb_enc_associate( str, rb_to_encoding( ptr->external));
    if (!NIL_P( ptr->internal))
        str = rb_str_conv_enc( str, rb_enc_get( str), rb_to_encoding( ptr->internal));
#endif
    return str;
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
    c->external = rb_enc_from_encoding( rb_default_external_encoding());
    c->internal = rb_enc_from_encoding( rb_default_internal_encoding());
#endif
    c->notice  = Qnil;
    return r;
}

/*
 * Document-method: Pg::Conn.connect
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
 * Document-method: Pg::Conn.new
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
 * If the +str+ argument is given but no further parameters,
 * the PQconnectdb() (not PQconnectdbParams()) function will be called.
 * If further arguments are present, the +str+ argument will be made into
 * the "dbname" parameter, but without overwriting an existing one.
 *
 * The "dbname" parameter may be a +conninfo+ string as described in the
 * PostgreSQL documentation:
 *
 *   c = Pg::Conn.new "postgresql://user:password@host:port/dbname"
 *   c = Pg::Conn.new "postgres://user:password@host:port/dbname"
 *   c = Pg::Conn.new "dbname=... host=... user=..."
 *
 * The password may be specified as an extra parameter:
 *
 *   c = Pg::Conn.new "postgresql://user@host/dbname", password: "verysecret"
 *
 * If the password is the empty string, and there is either an instance method
 * or a class method <code>password?</code>, that method will be asked. This
 * method may ask <code>yield :user</code> or <code>yield :dbname</code> and so
 * on to get the connection parameters.
 *
 *   class Pg::Conn
 *     def password?
 *       "tercesyrev".reverse
 *     end
 *   end
 *   c = Pg::Conn.new "postgresql://user@host/dbname", password: ""
 *
 * See the PostgreSQL documentation for a full list:
 * [http://www.postgresql.org/docs/current/interactive/libpq-connect.html#LIBPQ-PQCONNECTDBPARAMS]
 *
 * On failure, a +Pg::Error+ exception will be raised.
 *
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
        if (TYPE( str) == T_HASH) {
            params = str;
            str = Qnil;
        }

    Data_Get_Struct( self, struct pgconn_data, c);

    if (NIL_P( params)) {
        StringValue( str);
        c->conn = PQconnectdb( RSTRING_PTR( str));
    } else {
        int expand_dbname;
        VALUE orig_params = params;

        if (TYPE( params) != T_HASH)
            params = rb_convert_type( params, T_HASH, "Hash", "to_hash");

        if (!NIL_P( str) && NIL_P( rb_hash_aref( params, connstr_sym_dbname()))) {
            if (params == orig_params)
                params = rb_obj_dup( params);
            rb_hash_aset( params, sym_dbname, str);
            expand_dbname = 1;
        } else
            expand_dbname = 0;

        connstr_passwd( self, orig_params, &params);

        l = RHASH_SIZE( params) + 1;
        keywords = (const char **) ALLOCA_N( char *, l);
        values   = (const char **) ALLOCA_N( char *, l);
        ptrs[ 0] = keywords;
        ptrs[ 1] = values;
        ptrs[ 2] = (const char **) c;
        st_foreach( RHASH_TBL( params), &set_connect_params, (st_data_t) ptrs);
        *(ptrs[ 0]) = *(ptrs[ 1]) = NULL;

        c->conn = PQconnectdbParams( keywords, values, expand_dbname);
    }
    if (PQstatus( c->conn) == CONNECTION_BAD)
        rb_exc_raise( pgconnfailederror_new( c, params));

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

VALUE
connstr_sym_dbname( void)
{
    if (sym_dbname == Qundef)
        sym_dbname = ID2SYM( rb_intern( "dbname"));
    return sym_dbname;
}

VALUE
connstr_sym_password( void)
{
    if (sym_password == Qundef)
        sym_password = ID2SYM( rb_intern( "password"));
    return sym_password;
}


void
connstr_passwd( VALUE self, VALUE orig_params, VALUE *params)
{
    VALUE pw;

    pw = rb_hash_aref( *params, connstr_sym_password());
    if (TYPE( pw) == T_STRING && RSTRING_LEN( pw) == 0) {
        static ID id_password_q = 0;
        VALUE pwobj;

        if (id_password_q == 0)
            id_password_q = rb_intern( "password?");
        pwobj = Qundef;
        if (rb_respond_to( self, id_password_q))
            pwobj = self;
        if (rb_respond_to( CLASS_OF( self), id_password_q))
            pwobj = CLASS_OF( self);
        if (pwobj != Qundef) {
            if (*params == orig_params)
                *params = rb_obj_dup( *params);
            rb_hash_aset( *params, sym_password,
                rb_block_call( pwobj, id_password_q, 0, NULL,
                                                &connstr_getparam, *params));
        }
    }
}

VALUE
connstr_getparam( RB_BLOCK_CALL_FUNC_ARGLIST( yielded, params))
{
    return rb_hash_aref( params, yielded);
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
        rb_raise( rb_ePgError, "Invalid encoding name %s", RSTRING_PTR( str));
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
    return c->external;
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
    rb_encoding *e;

    e = NIL_P( enc) ? rb_to_encoding( enc) : rb_default_external_encoding();
    Data_Get_Struct( self, struct pgconn_data, c);
    c->external = rb_enc_from_encoding( e);

    return Qnil;
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
    return c->internal;
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
    rb_encoding *e;

    e = NIL_P( enc) ? rb_to_encoding( enc) : rb_default_internal_encoding();
    Data_Get_Struct( self, struct pgconn_data, c);
    c->internal = rb_enc_from_encoding( e);

    return Qnil;
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
pgconn_dbname( VALUE self)
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
 * == Example
 *
 *   conn.exec <<-EOT
 *     CREATE OR REPLACE FUNCTION noise() RETURNS VOID AS $$
 *     BEGIN
 *       RAISE NOTICE 'Hi!';
 *     END;
 *     $$ LANGUAGE plpgsql;
 *   EOT
 *   conn.on_notice { |e| puts e.inspect }
 *   conn.exec "SELECT noise();"
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

        err = pgconn_mkstring( c, PQresultErrorMessage( result));
        rb_proc_call( c->notice, rb_ary_new3( 1l, err));
    }
}




/*
 * Document-class: Pg::Conn::Failed
 *
 * Error while establishing a connection to the PostgreSQL server.
 */

/*
 * Document-class: Pg::Conn::Invalid
 *
 * Invalid (closed) connection.
 */


/*
 * Document-class: Pg::Conn
 *
 * The class to access a PostgreSQL database.
 *
 * == Example
 *
 *    require "pgsql"
 *    conn = Pg::Conn.open :dbname => "test1"
 *    res = conn.exec "SELECT * FROM mytable;"
 *
 * See the Pg::Result class for information on working with the results of a
 * query.
 */

void
Init_pgsql_conn( void)
{
    {
        ID id_require;

        id_require = rb_intern( "require");
        rb_funcall( Qnil, id_require, 1, rb_str_new2( "date"));
        rb_funcall( Qnil, id_require, 1, rb_str_new2( "time"));
        rb_funcall( Qnil, id_require, 1, rb_str_new2( "bigdecimal"));
    }

    rb_cPgConn = rb_define_class_under( rb_mPg, "Conn", rb_cObject);

    rb_ePgConnFailed  = rb_define_class_under( rb_cPgConn, "Failed",  rb_ePgError);
    rb_undef_method( CLASS_OF( rb_ePgConnFailed), "new");
    rb_define_attr( rb_ePgConnFailed, "parameters", 1, 0);

    rb_ePgConnInvalid = rb_define_class_under( rb_cPgConn, "Invalid", rb_ePgError);

    rb_define_alloc_func( rb_cPgConn, pgconn_alloc);
    rb_define_singleton_method( rb_cPgConn, "connect", pgconn_s_connect, -1);
    rb_define_alias( rb_singleton_class( rb_cPgConn), "open", "connect");
    rb_define_method( rb_cPgConn, "initialize", &pgconn_init, -1);
    rb_define_method( rb_cPgConn, "close", &pgconn_close, 0);
    rb_define_alias( rb_cPgConn, "finish", "close");
    rb_define_method( rb_cPgConn, "reset", &pgconn_reset, 0);

    rb_define_method( rb_cPgConn, "client_encoding", &pgconn_client_encoding, 0);
    rb_define_method( rb_cPgConn, "set_client_encoding", &pgconn_set_client_encoding, 1);
#ifdef RUBY_ENCODING
    rb_define_method( rb_cPgConn, "external_encoding",  &pgconn_externalenc, 0);
    rb_define_method( rb_cPgConn, "external_encoding=", &pgconn_set_externalenc, 1);
    rb_define_method( rb_cPgConn, "internal_encoding",  &pgconn_internalenc, 0);
    rb_define_method( rb_cPgConn, "internal_encoding=", &pgconn_set_internalenc, 1);
#endif

    rb_define_method( rb_cPgConn, "protocol_version", &pgconn_protocol_version, 0);
    rb_define_method( rb_cPgConn, "server_version", &pgconn_server_version, 0);

    rb_define_method( rb_cPgConn, "dbname", &pgconn_dbname, 0);
    rb_define_alias( rb_cPgConn, "db", "dbname");
    rb_define_method( rb_cPgConn, "host", &pgconn_host, 0);
    rb_define_method( rb_cPgConn, "options", &pgconn_options, 0);
    rb_define_method( rb_cPgConn, "port", &pgconn_port, 0);
    rb_define_method( rb_cPgConn, "tty", &pgconn_tty, 0);
    rb_define_method( rb_cPgConn, "user", &pgconn_user, 0);
    rb_define_method( rb_cPgConn, "status", &pgconn_status, 0);
    rb_define_method( rb_cPgConn, "error", &pgconn_error, 0);

#define CONN_DEF( c) rb_define_const( rb_cPgConn, #c, INT2FIX( CONNECTION_ ## c))
    CONN_DEF( OK);
    CONN_DEF( BAD);
#undef CONN_DEF

    rb_define_method( rb_cPgConn, "socket", &pgconn_socket, 0);

    rb_define_method( rb_cPgConn, "trace", &pgconn_trace, -1);
    rb_define_method( rb_cPgConn, "untrace", &pgconn_untrace, 0);

    rb_define_method( rb_cPgConn, "on_notice", &pgconn_on_notice, 0);

    Init_pgsql_conn_quote();
    Init_pgsql_conn_exec();
}

