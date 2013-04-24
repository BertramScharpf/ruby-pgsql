/*
 *  conn_exec.c  --  PostgreSQL connection, command execution
 */


#include "conn_exec.h"

#include "conn_quote.h"
#include "result.h"


#ifdef HAVE_FUNC_RB_ERRINFO
    #define RB_ERRINFO (rb_errinfo())
#else
    #define RB_ERRINFO ruby_errinfo
#endif


static void pg_raise_connexec( struct pgconn_data *c);

static PGresult *pg_statement_exec( VALUE conn, VALUE cmd, VALUE par);
static void      pg_statement_send( VALUE conn, VALUE cmd, VALUE par);
static char **params_to_strings( VALUE conn, VALUE params, int *len);
static void free_strings( char **strs, int len);
static void pg_parse_parameters( int argc, VALUE *argv, VALUE *cmd, VALUE *par);

static VALUE pgconn_exec( int argc, VALUE *argv, VALUE obj);
static VALUE pgconn_send( int argc, VALUE *argv, VALUE obj);
static VALUE pgconn_fetch( VALUE obj);
static VALUE yield_or_return_result( VALUE res);
static VALUE clear_resultqueue( VALUE self);

static VALUE pgconn_query(         int argc, VALUE *argv, VALUE self);
static VALUE pgconn_select_one(    int argc, VALUE *argv, VALUE self);
static VALUE pgconn_select_value(  int argc, VALUE *argv, VALUE self);
static VALUE pgconn_select_values( int argc, VALUE *argv, VALUE self);
static VALUE pgconn_get_notify( VALUE self);

static VALUE pgconn_transaction( int argc, VALUE *argv, VALUE self);
static VALUE rescue_transaction( VALUE self);
static VALUE yield_transaction( VALUE self);
static VALUE pgconn_subtransaction( int argc, VALUE *argv, VALUE self);
static VALUE rescue_subtransaction( VALUE ary);
static VALUE yield_subtransaction( VALUE ary);
static VALUE pgconn_transaction_status( VALUE self);



static VALUE rb_ePgConnExec;
static VALUE rb_ePgConnTrans;



void
pg_raise_connexec( struct pgconn_data *c)
{
    rb_raise( rb_ePgConnExec, PQerrorMessage( c->conn));
}


PGresult *
pg_statement_exec( VALUE conn, VALUE cmd, VALUE par)
{
    struct pgconn_data *c;
    PGresult *result;

    Data_Get_Struct( conn, struct pgconn_data, c);
    pg_check_conninvalid( c);
    if (NIL_P( par))
        result = PQexec( c->conn, pgconn_destring( c, cmd, NULL));
    else {
        char **v;
        int len;

        v = params_to_strings( conn, par, &len);
        result = PQexecParams( c->conn, pgconn_destring( c, cmd, NULL), len,
                               NULL, (const char **) v, NULL, NULL, 0);
        free_strings( v, len);
    }
    if (result == NULL)
        pg_raise_connexec( c);
    c->command = cmd;
    c->params  = par;
    pg_checkresult( result, c);
    return result;
}


void
pg_statement_send( VALUE conn, VALUE cmd, VALUE par)
{
    struct pgconn_data *c;
    int res;

    Data_Get_Struct( conn, struct pgconn_data, c);
    pg_check_conninvalid( c);
    if (NIL_P( par))
        res = PQsendQuery( c->conn, pgconn_destring( c, cmd, NULL));
    else {
        char **v;
        int len;

        v = params_to_strings( conn, par, &len);
        res = PQsendQueryParams( c->conn, pgconn_destring( c, cmd, NULL), len,
                                 NULL, (const char **) v, NULL, NULL, 0);
        free_strings( v, len);
    }
    if (res <= 0)
        pg_raise_connexec( c);
    c->command = cmd;
    c->params  = par;
}

char **
params_to_strings( VALUE conn, VALUE params, int *len)
{
    struct pgconn_data *c;
    VALUE *ptr;
    int l;
    char **values, **v;
    char *a;

    Data_Get_Struct( conn, struct pgconn_data, c);
    ptr = RARRAY_PTR( params);
    *len = l = RARRAY_LEN( params);
    values = ALLOC_N( char *, l);
    for (v = values; l; v++, ptr++, l--)
        if (NIL_P( *ptr))
            *v = NULL;
        else {
            const char *q;
            char *p;
            int n;

            q = pgconn_destring( c, pgconn_stringize( conn, *ptr), &n);
            a = ALLOC_N( char, n + 1);
            for (p = a; *p = *q; ++p, ++q)
                ;
            *v = a;
        }
    return values;
}

void
free_strings( char **strs, int len)
{
    char **p;
    int l;

    for (p = strs, l = len; l; --l, ++p)
        xfree( *p);
    xfree( strs);
}


void
pg_parse_parameters( int argc, VALUE *argv, VALUE *cmd, VALUE *par)
{
    int len;

    rb_scan_args( argc, argv, "1*", cmd, par);
    StringValue( *cmd);
    if (RARRAY_LEN( *par) <= 0)
        *par = Qnil;
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
pgconn_exec( int argc, VALUE *argv, VALUE self)
{
    struct pgconn_data *c;
    VALUE cmd, par;
    PGresult *res;

    pg_parse_parameters( argc, argv, &cmd, &par);
    res = pg_statement_exec( self, cmd, par);
    Data_Get_Struct( self, struct pgconn_data, c);
    return yield_or_return_result( pgresult_new( res, c));
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
pgconn_send( int argc, VALUE *argv, VALUE self)
{
    VALUE cmd, par;

    pg_parse_parameters( argc, argv, &cmd, &par);
    pg_statement_send( self, cmd, par);
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
pgconn_fetch( VALUE self)
{
    struct pgconn_data *c;
    PGresult *result;
    VALUE res;

    Data_Get_Struct( self, struct pgconn_data, c);
    pg_check_conninvalid( c);
    if (PQconsumeInput( c->conn) == 0)
        pg_raise_connexec( c);
    if (PQisBusy( c->conn) > 0)
        return Qnil;
    result = PQgetResult( c->conn);
    if (result == NULL)
        res = Qnil;
    else {
        pg_checkresult( result, c);
        res = pgresult_new( result, c);
    }
    return yield_or_return_result( res);
}

VALUE
yield_or_return_result( VALUE result)
{
    struct pgresult_data *r;

    Data_Get_Struct( result, struct pgresult_data, r);
    pgconn_clear( r->conn);
    return RTEST( rb_block_given_p()) ?
        rb_ensure( rb_yield, result, pgresult_clear, result) : result;
}

VALUE
clear_resultqueue( VALUE self)
{
    struct pgconn_data *c;
    PGresult *result;

    Data_Get_Struct( self, struct pgconn_data, c);
    while ((result = PQgetResult( c->conn)) != NULL)
        PQclear( result);
    pgconn_clear( c);
    return Qnil;
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
pgconn_query( int argc, VALUE *argv, VALUE self)
{
    struct pgconn_data *c;
    VALUE cmd, par;
    PGresult *result;

    pg_parse_parameters( argc, argv, &cmd, &par);
    result = pg_statement_exec( self, cmd, par);
    Data_Get_Struct( self, struct pgconn_data, c);
    pgconn_clear( c);
    if (rb_block_given_p()) {
        VALUE res;

        res = pgresult_new( result, c);
        return rb_ensure( pgresult_each, res, pgresult_clear, res);
    } else {
        struct pgresult_data r;
        int m, j;
        VALUE ret;

        pgresult_init( &r, result, c);
        m = PQntuples( result);
        ret = rb_ary_new2( m);
        for (j = 0; m; ++j, --m)
            rb_ary_store( ret, j, pg_fetchrow( Qnil, &r, j));
        PQclear( result);
        return ret;
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
pgconn_select_one( int argc, VALUE *argv, VALUE self)
{
    struct pgconn_data *c;
    VALUE cmd, par;
    struct pgresult_data r;
    VALUE row;

    pg_parse_parameters( argc, argv, &cmd, &par);
    Data_Get_Struct( self, struct pgconn_data, c);
    pgresult_init( &r, pg_statement_exec( self, cmd, par), c);
    pgconn_clear( c);
    row = PQntuples( r.res) ? pg_fetchrow( Qnil, &r, 0)
                            : Qnil;
    PQclear( r.res);
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
pgconn_select_value( int argc, VALUE *argv, VALUE self)
{
    struct pgconn_data *c;
    VALUE cmd, par;
    struct pgresult_data r;
    VALUE ret;

    pg_parse_parameters( argc, argv, &cmd, &par);
    Data_Get_Struct( self, struct pgconn_data, c);
    pgresult_init( &r, pg_statement_exec( self, cmd, par), c);
    pgconn_clear( c);
    ret = PQntuples( r.res) ? pg_fetchresult( &r, 0, 0)
                            : Qnil;
    PQclear( r.res);
    return ret;
}

/*
 * call-seq:
 *   conn.select_values( query, *bind_values)
 *
 * Equivalent to conn.query( query, *bind_values).flatten
 */
VALUE
pgconn_select_values( int argc, VALUE *argv, VALUE self)
{
    struct pgconn_data *c;
    VALUE cmd, par;
    struct pgresult_data r;
    int n, m, n_;
    int i, j, k;
    VALUE ret;

    pg_parse_parameters( argc, argv, &cmd, &par);
    Data_Get_Struct( self, struct pgconn_data, c);
    pgresult_init( &r, pg_statement_exec( self, cmd, par), c);
    pgconn_clear( c);

    m = PQntuples( r.res), n = PQnfields( r.res);
    ret = rb_ary_new2( m * n);
    n_ = n;
    for (k = 0, j = 0; m; ++j, --m) {
        for (i = 0; n; ++i, --n, ++k)
            rb_ary_store( ret, k, pg_fetchresult( &r, j, i));
        n = n_;
    }
    PQclear( r.res);
    return ret;
}

/*
 * call-seq:
 *    conn.get_notify()  -> ary or nil
 *    conn.get_notify() { |rel,pid,msg| .... } -> obj
 *
 * Returns a notifier. If there is no unprocessed notifier, it returns +nil+.
 */
VALUE
pgconn_get_notify( VALUE self)
{
    struct pgconn_data *c;
    PGnotify *notify;
    VALUE rel, pid, ext;
    VALUE ret;

    Data_Get_Struct( self, struct pgconn_data, c);
    if (PQconsumeInput( c->conn) == 0)
        pg_raise_connexec( c);
    notify = PQnotifies( c->conn);
    if (notify == NULL)
        return Qnil;
    rel = pgconn_mkstring( c, notify->relname);
    pid = INT2FIX( notify->be_pid),
    ext = pgconn_mkstring( c, notify->extra);
    ret = rb_ary_new3( 3, rel, pid, ext);
    PQfreemem( notify);
    return RTEST( rb_block_given_p()) ? rb_yield( ret) : ret;
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
    struct pgconn_data *c;
    VALUE ser, ro;
    VALUE cmd;
    int p;

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

    Data_Get_Struct( self, struct pgconn_data, c);
    if (PQtransactionStatus( c->conn) > PQTRANS_IDLE)
        rb_raise( rb_ePgConnTrans,
            "Nested transaction block. Use Conn#subtransaction.");
    pg_statement_exec( self, cmd, Qnil);
    return rb_rescue( yield_transaction, self, rescue_transaction, self);
}

VALUE
rescue_transaction( VALUE self)
{
    pg_statement_exec( self, rb_str_new2( "rollback;"), Qnil);
    rb_exc_raise( RB_ERRINFO);
    return Qnil;
}

VALUE
yield_transaction( VALUE self)
{
    VALUE r;

    r = rb_yield( self);
    pg_statement_exec( self, rb_str_new2( "commit;"), Qnil);
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
    struct pgconn_data *c;
    int a;
    VALUE sp, par, cmd, ya;
    char *p;

    Data_Get_Struct( self, struct pgconn_data, c);
    a = rb_scan_args( argc, argv, "1*", &sp, &par);
    StringValue( sp);
    if (a > 1)
        sp = rb_str_format(RARRAY_LEN(par), RARRAY_PTR(par), sp);

    cmd = rb_str_buf_new2( "savepoint ");
    p = PQescapeIdentifier( c->conn, RSTRING_PTR( sp), RSTRING_LEN( sp));
    rb_str_buf_cat2( cmd, p);
    ya = rb_ary_new3( 2, self, rb_str_new2( p));
    PQfreemem( p);
    rb_str_buf_cat2( cmd, ";");

    pg_statement_exec( self, cmd, Qnil);
    return rb_rescue( yield_subtransaction, ya, rescue_subtransaction, ya);
}

VALUE
rescue_subtransaction( VALUE ary)
{
    VALUE cmd;

    cmd = rb_str_buf_new2( "rollback to savepoint ");
    rb_str_buf_append( cmd, rb_ary_entry( ary, 1));
    rb_str_buf_cat2( cmd, ";");
    pg_statement_exec( rb_ary_entry( ary, 0), cmd, Qnil);
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
    pg_statement_exec( rb_ary_entry( ary, 0), cmd, Qnil);
    return r;
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
    struct pgconn_data *c;

    Data_Get_Struct( self, struct pgconn_data, c);
    return INT2FIX( PQtransactionStatus( c->conn));
}






/********************************************************************
 *
 * Document-class: Pg::Conn::Exec
 *
 * Error while querying from a PostgreSQL connection.
 */


/********************************************************************
 *
 * Document-class: Pg::Conn::Transaction
 *
 * Nested transaction blocks. Use savepoints.
 */


void
Init_pgsql_conn_exec( void)
{

#define ERR_DEF( n)  rb_define_class_under( rb_cPgConn, n, rb_ePgError)
    rb_ePgConnExec    = ERR_DEF( "Exec");
#undef ERR_DEF

    rb_define_method( rb_cPgConn, "exec", pgconn_exec, -1);
    rb_define_method( rb_cPgConn, "send", pgconn_send, -1);
    rb_define_method( rb_cPgConn, "fetch", pgconn_fetch, 0);

    rb_define_method( rb_cPgConn, "query", pgconn_query, -1);
    rb_define_method( rb_cPgConn, "select_one", pgconn_select_one, -1);
    rb_define_method( rb_cPgConn, "select_value", pgconn_select_value, -1);
    rb_define_method( rb_cPgConn, "select_values", pgconn_select_values, -1);
    rb_define_method( rb_cPgConn, "get_notify", pgconn_get_notify, 0);


#define ERR_DEF( n)  rb_define_class_under( rb_cPgConn, n, rb_ePgError)
    rb_ePgConnTrans = ERR_DEF( "Transaction");
#undef ERR_DEF

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
    rb_define_method( rb_cPgConn, "transaction_status", pgconn_transaction_status, 0);
}

