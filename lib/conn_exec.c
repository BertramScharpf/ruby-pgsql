/*
 *  conn_exec.c  --  PostgreSQL connection, command execution
 */


#include "conn_exec.h"

#include "conn_quote.h"
#include "result.h"

#include <math.h>


static void pg_raise_connexec( struct pgconn_data *c);

static VALUE pg_statement_exec( VALUE conn, VALUE cmd, VALUE par);
static void  pg_statement_send( VALUE conn, VALUE cmd, VALUE par);
static char **params_to_strings( VALUE conn, VALUE params, int *len);
static void free_strings( char **strs, int len);
static void pg_parse_parameters( int argc, VALUE *argv, VALUE *cmd, VALUE *par);

static VALUE pgconn_exec( int argc, VALUE *argv, VALUE obj);
static VALUE yield_or_return_result( VALUE res);
static VALUE pgconn_send( int argc, VALUE *argv, VALUE obj);
static VALUE pgconn_fetch( int argc, VALUE *argv, VALUE conn);
static void wait_for_pgsocket( PGconn *c, VALUE to);
static VALUE clear_resultqueue( VALUE self);
static VALUE pgconn_fetch_rows( int argc, VALUE *argv, VALUE conn);
static VALUE fetch_result_each( RB_BLOCK_CALL_FUNC_ARGLIST( res, arg));

static VALUE pgconn_query(         int argc, VALUE *argv, VALUE self);
static VALUE pgconn_select_row(    int argc, VALUE *argv, VALUE self);
static VALUE pgconn_select_value(  int argc, VALUE *argv, VALUE self);
static VALUE pgconn_select_values( int argc, VALUE *argv, VALUE self);
static VALUE pgconn_get_notify( VALUE self);

static VALUE pgconn_transaction( int argc, VALUE *argv, VALUE self);
static VALUE rollback_transaction( VALUE conn, VALUE err);
static VALUE commit_transaction( VALUE self);
static VALUE yield_transaction( VALUE conn);
static VALUE pgconn_subtransaction( int argc, VALUE *argv, VALUE self);
static VALUE rollback_subtransaction( VALUE ary, VALUE err);
static VALUE release_subtransaction( VALUE ary);
static VALUE yield_subtransaction( VALUE ary);
static VALUE pgconn_transaction_status( VALUE self);


static VALUE pgconn_copy_stdin( int argc, VALUE *argv, VALUE self);
static VALUE put_end( VALUE conn);
static VALUE pgconn_putline( VALUE self, VALUE str);
static VALUE pgconn_copy_stdout( int argc, VALUE *argv, VALUE self);
static VALUE get_end( VALUE conn);
static VALUE pgconn_getline( int argc, VALUE *argv, VALUE self);
static VALUE pgconn_each_line( VALUE self);

static VALUE pgconn_backup( VALUE self, VALUE label);
static VALUE backup_end( VALUE conn);


static VALUE rb_ePgConnExec;
static VALUE rb_ePgConnTimeout;
static VALUE rb_ePgConnTrans;
static VALUE rb_ePgConnCopy;

static ID id_to_a;
static ID id_fetch;


void
pg_raise_connexec( struct pgconn_data *c)
{
    rb_raise( rb_ePgConnExec, "%s", PQerrorMessage( c->conn));
}


VALUE
pg_statement_exec( VALUE conn, VALUE cmd, VALUE par)
{
    struct pgconn_data *c;
    PGresult *result;

    c = get_pgconn( conn);
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
    return pgresult_new( result, conn, cmd, par);
}


void
pg_statement_send( VALUE conn, VALUE cmd, VALUE par)
{
    struct pgconn_data *c;
    int res;

    c = get_pgconn( conn);
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
    PQsetSingleRowMode( c->conn);
}

char **
params_to_strings( VALUE conn, VALUE params, int *len)
{
    struct pgconn_data *c;
    VALUE *ptr;
    int l;
    char **values, **v;
    char *a;

    c = get_pgconn( conn);
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
            for (p = a; (*p = n ? *q : '\0'); ++p, ++q, --n)
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
    VALUE cmd, par;
    VALUE res;

    pg_parse_parameters( argc, argv, &cmd, &par);
    res = pg_statement_exec( self, cmd, par);
    return yield_or_return_result( res);
}

VALUE
yield_or_return_result( VALUE result)
{
    return rb_block_given_p() ?
        rb_ensure( rb_yield, result, pgresult_clear, result) : result;
}


/*
 * call-seq:
 *    conn.send( sql, *bind_values) { |conn| ... }  -> nil
 *
 * Sends an asynchronous SQL query request specified by +sql+ to the
 * PostgreSQL server.
 *
 * This sets the query into single row mode. You have to call +Pg::Conn#fetch+
 * what will yield one-row results. You may cancel the delivery by breaking
 * the loop.
 *
 * Use Pg::Conn#fetch to fetch the results after you waited for data.
 *
 *   Pg::Conn.connect do |conn|
 *     conn.send "SELECT pg_sleep(3), * FROM t;" do
 *       conn.fetch { |res|
 *         puts res.first.inspect
 *         break if (rand 3) < 1
 *       }
 *     end
 *   end
 *
 * Multiple select statements will be separated by an empty result.
 *
 *   Pg::Conn.connect do |conn|
 *     conn.send "SELECT 33; SELECT 'foo';" do
 *       conn.fetch { |res|
 *         puts res.first.inspect
 *       }
 *     end
 *   end
 */
VALUE
pgconn_send( int argc, VALUE *argv, VALUE self)
{
    VALUE cmd, par;

    pg_parse_parameters( argc, argv, &cmd, &par);
    pg_statement_send( self, cmd, par);
    return rb_ensure( rb_yield, self, clear_resultqueue, self);
}

/*
 * call-seq:
 *    conn.fetch( timeout = nil) { |result| ... }  -> obj
 *
 * Fetches the results of the previous Pg::Conn#send call.
 * See there for an example.
 *
 */
VALUE
pgconn_fetch( int argc, VALUE *argv, VALUE conn)
{
    struct pgconn_data *c;
    PGresult *result;
    VALUE to;

    rb_scan_args( argc, argv, "01", &to);

    c = get_pgconn( conn);
    wait_for_pgsocket( c->conn, to);
    if (PQconsumeInput( c->conn) == 0)
        pg_raise_connexec( c);
    if (PQisBusy( c->conn) == 0)
        while ((result = PQgetResult( c->conn)) != NULL) {
            VALUE res;

            res = pgresult_new( result, conn, Qnil, Qnil);
            rb_ensure( rb_yield, res, pgresult_clear, res);
        }
    return Qnil;
}

void
wait_for_pgsocket( PGconn *c, VALUE to)
{
    int fd;
    fd_set readset;
    struct timeval tv, *ptv;

    fd = PQsocket( c);

    FD_ZERO(&readset);
    FD_SET(fd, &readset);

    ptv = NULL;
    if (!NIL_P( to)) {
        int type = TYPE( to);
        if (type == T_FIXNUM) {
            tv.tv_sec = FIX2LONG( to);
            tv.tv_usec = 0;
        } else {
            double x;
            switch (type) {
            case T_FLOAT:
                x = RFLOAT_VALUE( to);
                break;
            case T_BIGNUM:
                x = rb_big2dbl( to);
                break;
            case T_RATIONAL:
                x = rb_num2dbl( to);
                break;
            default:
                x = RFLOAT_VALUE( rb_funcall( to, rb_intern( "to_f"), 0));
                break;
            }
            tv.tv_sec  = floor( x);
            tv.tv_usec = round( (x-tv.tv_sec)*1000000);
        }
        ptv = &tv;
    }

    if (select( fd+1, &readset, NULL, NULL, ptv) < 0 || !FD_ISSET( fd, &readset))
        rb_raise( rb_ePgConnTimeout, "Wait for data timed out.");
}

VALUE
clear_resultqueue( VALUE conn)
{
    struct pgconn_data *c;
    PGresult *result;
    int cancelled;

    c = get_pgconn( conn);
    cancelled = 0;
    while ((result = PQgetResult( c->conn)) != NULL) {
        PQclear( result);
        if (!cancelled) {
            char errbuf[ 256];
            PGcancel *cancel;
            int ret;

            cancel = PQgetCancel( c->conn);
            if (cancel == NULL)
                rb_raise( rb_ePgConnTrans, "Could not get cancel object.");
            ret = PQcancel( cancel, errbuf, sizeof errbuf);
            PQfreeCancel( cancel);
            if (ret == 0)
                rb_raise( rb_ePgConnTrans, "Cancel of sent query failed: %s", errbuf);
            cancelled = 1;
        }
    }
    return Qnil;
}


/*
 * call-seq:
 *    conn.fetch_rows( timeout = nil) { |row| ... }  -> obj
 *
 * Fetches the results of the previous Pg::Conn#send call as an array.
 *
 * Multiple select statements will _not_ be separated by an empty array or
 * something similar. Query twice or call +#fetch+ if you want to separate
 * them.
 *
 *   Pg::Conn.connect do |conn|
 *     conn.send "SELECT 33; SELECT 'foo';" do
 *       conn.fetch { |row|
 *         puts row.inspect
 *       }
 *     end
 *   end
 */
VALUE
pgconn_fetch_rows( int argc, VALUE *argv, VALUE conn)
{
    if (!id_fetch)
        id_fetch = rb_intern( "fetch");
    return rb_block_call( conn, id_fetch, argc, argv, fetch_result_each, Qnil);
}

VALUE
fetch_result_each( RB_BLOCK_CALL_FUNC_ARGLIST( res, arg))
{
    return pgresult_each( res);
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
    VALUE cmd, par;
    VALUE res;

    pg_parse_parameters( argc, argv, &cmd, &par);
    res = pg_statement_exec( self, cmd, par);
    if (rb_block_given_p())
        return rb_ensure( pgresult_each, res, pgresult_clear, res);
    else {
        VALUE ret;

        if (!id_to_a)
            id_to_a = rb_intern( "to_a");
        ret = rb_funcall( res, id_to_a, 0);
        pgresult_clear( res);
        return ret;
    }
}

/*
 * call-seq:
 *   conn.select_row( query, *bind_values)
 *
 * Return the first row of the query results.
 * Equivalent to <code>conn.query( query, *bind_values).first</code>.
 */
VALUE
pgconn_select_row( int argc, VALUE *argv, VALUE self)
{
    VALUE cmd, par;
    VALUE res;
    struct pgresult_data *r;

    pg_parse_parameters( argc, argv, &cmd, &par);
    res = pg_statement_exec( self, cmd, par);

    TypedData_Get_Struct( res, struct pgresult_data, &pgresult_data_data_type, r);
    return pg_fetchrow( r, 0);
}

/*
 * call-seq:
 *   conn.select_value( query, *bind_values)
 *
 * Return the first value of the first row of the query results.
 * Equivalent to conn.query( query, *bind_values).first&.first
 */
VALUE
pgconn_select_value( int argc, VALUE *argv, VALUE self)
{
    VALUE cmd, par;
    VALUE res;
    struct pgresult_data *r;

    pg_parse_parameters( argc, argv, &cmd, &par);
    res = pg_statement_exec( self, cmd, par);

    TypedData_Get_Struct( res, struct pgresult_data, &pgresult_data_data_type, r);
    return PQntuples( r->res) > 0 && PQnfields( r->res) > 0 ?
                    pg_fetchresult( r, 0, 0) : Qnil;
}

/*
 * call-seq:
 *   conn.select_values( query, *bind_values)
 *
 * Return the all values over all rows as one array.
 * Equivalent to conn.query( query, *bind_values).flatten
 */
VALUE
pgconn_select_values( int argc, VALUE *argv, VALUE self)
{
    VALUE cmd, par;
    VALUE res;
    struct pgresult_data *r;
    int n, m, n_, l;
    int i, j, k;
    VALUE ret;

    pg_parse_parameters( argc, argv, &cmd, &par);
    res = pg_statement_exec( self, cmd, par);

    TypedData_Get_Struct( res, struct pgresult_data, &pgresult_data_data_type, r);
    m = PQntuples( r->res), n = PQnfields( r->res);
    l = m * n;
    if (l == 0)
        return Qnil;
    ret = rb_ary_new2( l);
    n_ = n;
    for (k = 0, j = 0; m; ++j, --m) {
        for (i = 0; n; ++i, --n, ++k)
            rb_ary_store( ret, k, pg_fetchresult( r, j, i));
        n = n_;
    }
    return ret;
}

/*
 * call-seq:
 *    conn.get_notify()  -> ary or nil
 *    conn.get_notify() { |rel,pid,msg| .... } -> obj
 *
 * Returns a notifier.  If there is no unprocessed notifier, it returns +nil+.
 */
VALUE
pgconn_get_notify( VALUE conn)
{
    struct pgconn_data *c;
    PGnotify *notify;
    VALUE rel, pid, ext;
    VALUE ret;

    c = get_pgconn( conn);
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
    return rb_block_given_p() ? rb_yield( ret) : ret;
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
pgconn_transaction( int argc, VALUE *argv, VALUE conn)
{
    struct pgconn_data *c;
    VALUE ser, ro;
    VALUE cmd;
    int p;

    rb_scan_args( argc, argv, "02", &ser, &ro);
    cmd = rb_str_buf_new2( "BEGIN");
    p = 0;
    if (!NIL_P( ser)) {
        rb_str_buf_cat2( cmd, " ISOLATION LEVEL ");
        rb_str_buf_cat2( cmd, RTEST(ser) ? "SERIALIZABLE" : "READ COMMITTED");
        p++;
    }
    if (!NIL_P( ro)) {
        if (p) rb_str_buf_cat2( cmd, ",");
        rb_str_buf_cat2( cmd, " READ ");
        rb_str_buf_cat2( cmd, (RTEST(ro)  ? "ONLY" : "WRITE"));
    }
    rb_str_buf_cat2( cmd, ";");

    c = get_pgconn( conn);
    if (PQtransactionStatus( c->conn) > PQTRANS_IDLE)
        rb_raise( rb_ePgConnTrans,
            "Nested transaction block. Use Conn#subtransaction.");
    pgresult_clear( pg_statement_exec( conn, cmd, Qnil));
    return rb_ensure( yield_transaction, conn, commit_transaction, conn);
}

VALUE
yield_transaction( VALUE conn)
{
    return rb_rescue( rb_yield, conn, rollback_transaction, conn);
}

VALUE
rollback_transaction( VALUE conn, VALUE err)
{
    pgresult_clear( pg_statement_exec( conn, rb_str_new2( "ROLLBACK;"), Qnil));
    rb_exc_raise( err);
    return Qnil;
}

VALUE
commit_transaction( VALUE conn)
{
    struct pgconn_data *c;

    c = get_pgconn( conn);
    if (PQtransactionStatus( c->conn) > PQTRANS_IDLE)
        pgresult_clear( pg_statement_exec( conn, rb_str_new2( "COMMIT;"), Qnil));
    return Qnil;
}


/*
 * call-seq:
 *    conn.subtransaction( name, *args) { |conn,sp| ... }
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
    const char *q;
    char *p;
    int n;

    c = get_pgconn( self);
    a = rb_scan_args( argc, argv, "1*", &sp, &par);
    StringValue( sp);
    if (a > 1)
        sp = rb_str_format(RARRAY_LEN(par), RARRAY_PTR(par), sp);

    cmd = rb_str_buf_new2( "SAVEPOINT ");
    q = pgconn_destring( c, sp, &n);
    p = PQescapeIdentifier( c->conn, q, n);
    rb_str_buf_cat2( cmd, p);
    ya = rb_ary_new3( 2, self, rb_str_new2( p));
    PQfreemem( p);
    rb_str_buf_cat2( cmd, ";");

    pgresult_clear( pg_statement_exec( self, cmd, Qnil));
    return rb_ensure( yield_subtransaction, ya, release_subtransaction, ya);
}

VALUE
yield_subtransaction( VALUE ary)
{
    return rb_rescue( rb_yield, ary, rollback_subtransaction, ary);
}

VALUE
rollback_subtransaction( VALUE ary, VALUE err)
{
    VALUE cmd;

    cmd = rb_str_buf_new2( "ROLLBACK TO SAVEPOINT ");
    rb_str_buf_append( cmd, rb_ary_entry( ary, 1));
    rb_str_buf_cat2( cmd, ";");
    pgresult_clear( pg_statement_exec( rb_ary_entry( ary, 0), cmd, Qnil));
    rb_ary_store( ary, 1, Qnil);
    rb_exc_raise( err);
    return Qnil;
}

VALUE
release_subtransaction( VALUE ary)
{
    VALUE cmd;
    VALUE n;

    n = rb_ary_entry( ary, 1);
    if (!NIL_P( n)) {
        cmd = rb_str_buf_new2( "RELEASE SAVEPOINT ");
        rb_str_buf_append( cmd, n);
        rb_str_buf_cat2( cmd, ";");
        pgresult_clear( pg_statement_exec( rb_ary_entry( ary, 0), cmd, Qnil));
    }
    return Qnil;
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

    c = get_pgconn( self);
    return INT2FIX( PQtransactionStatus( c->conn));
}



/*
 * call-seq:
 *    conn.copy_stdin( sql, *bind_values) { |result| ... }   ->  nil
 *
 * Write lines into a +COPY+ command.  See +stringize_line+ for how to build
 * standard lines.
 *
 *   conn.copy_stdin "COPY t FROM STDIN;" do
 *      ary = ...
 *      l = conn.stringize_line ary
 *      conn.put l
 *   end
 *
 * You may write a "\\." yourself if you like it.
 */
VALUE
pgconn_copy_stdin( int argc, VALUE *argv, VALUE self)
{
    VALUE cmd, par;
    VALUE res;

    pg_parse_parameters( argc, argv, &cmd, &par);
    res = pg_statement_exec( self, cmd, par);
    return rb_ensure( rb_yield, res, put_end, self);
}


VALUE
put_end( VALUE self)
{
    struct pgconn_data *c;
    int r;
    PGresult *res;

    c = get_pgconn( self);
    /*
     * I would like to hand over something like
     *     RSTRING_PTR( rb_obj_as_string( rb_errinfo()))
     * here but when execution is inside a rescue block
     * the error info will be non-null even though the
     * exception just has been caught.
     */
    while ((r = PQputCopyEnd( c->conn, NULL)) == 0)
        ;
    if (r < 0)
        rb_raise( rb_ePgConnCopy, "Copy from stdin failed to finish.");
    while ((res = PQgetResult( c->conn)) != NULL)
        pgresult_new( res, self, Qnil, Qnil);
    return Qnil;
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
 * If +str+ doesn't end in a newline, one is appended.  If the argument
 * is +ary+, a line will be built using +stringize_line+.
 *
 * If the connection is in nonblocking mode and no data could be sent
 * the closure will be called and its value will be returned.
 */
VALUE
pgconn_putline( VALUE self, VALUE arg)
{
    struct pgconn_data *c;
    VALUE str;
    const char *p;
    int l;
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
    if (RSTRING_PTR( str)[ RSTRING_LEN( str) - 1] != '\n') {
        VALUE t;

        t = rb_str_dup( str);
        rb_str_buf_cat( t, "\n", 1);
        str = t;
    }

    c = get_pgconn( self);
    p = pgconn_destring( c, str, &l);
    r = PQputCopyData( c->conn, p, l);
    if (r < 0)
        rb_raise( rb_ePgConnCopy, "Copy from stdin failed.");
    else if (r == 0)
        return rb_yield( Qnil);
    return Qnil;
}


/*
 * call-seq:
 *    conn.copy_stdout( sql, *bind_values) { ... }   ->  nil
 *
 * Read lines from a +COPY+ command.  The form of the lines depends
 * on the statement's parameters.
 *
 *   conn.copy_stdout "COPY t TO STDOUT;" do
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
    VALUE cmd, par;
    VALUE res;

    pg_parse_parameters( argc, argv, &cmd, &par);
    res = pg_statement_exec( self, cmd, par);
    return rb_ensure( rb_yield, res, get_end, self);
}

VALUE
get_end( VALUE self)
{
    struct pgconn_data *c;
    PGresult *res;

    c = get_pgconn( self);
    if ((res = PQgetResult( c->conn)) != NULL)
        pgresult_new( res, self, Qnil, Qnil);
    return Qnil;
}

/*
 * call-seq:
 *    conn.getline( async = nil)         -> str
 *    conn.getline( async = nil) { ... } -> str
 *
 * Reads a line from the backend server after a +COPY+ command.
 * Returns +nil+ for EOF.
 *
 * If async is +true+ and no data is available then the block will be called
 * and its value will be returned.
 *
 * Call this method inside a block passed to +copy_stdout+.  See
 * there for an example.
 */
VALUE
pgconn_getline( int argc, VALUE *argv, VALUE self)
{
    struct pgconn_data *c;
    VALUE as;
    int async;
    char *b;
    int r;

    async = rb_scan_args( argc, argv, "01", &as) > 0 && !NIL_P( as) ? 1 : 0;

    c = get_pgconn( self);
    r = PQgetCopyData( c->conn, &b, async);
    if (r > 0) {
        VALUE ret;

        ret = pgconn_mkstringn( c, b, r);
        PQfreemem( b);
        rb_lastline_set( ret);
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
 * Call this method inside a block passed to +copy_stdout+.  See
 * there for an example.
 */
VALUE
pgconn_each_line( VALUE self)
{
    struct pgconn_data *c;
    char *b;
    int r;
    VALUE s;

    c = get_pgconn( self);
    for (; (r = PQgetCopyData( c->conn, &b, 0)) > 0;) {
        s = pgconn_mkstringn( c, b, r);
        PQfreemem( b);
        rb_yield( s);
    }
    return Qnil;
}



/*
 * call-seq:
 *    conn.backup( label) { |result| ... }   ->  nil
 *
 * Call the pg_start_backup() and pg_stop_backup() functions.
 */
VALUE
pgconn_backup( VALUE self, VALUE label)
{
    VALUE cmd, arg;

    cmd = rb_str_new2( "SELECT pg_start_backup($1);");
    arg = rb_ary_new3( 1, label);
    pgresult_clear( pg_statement_exec( self, cmd, arg));
    return rb_ensure( rb_yield, Qnil, backup_end, self);
}


VALUE
backup_end( VALUE self)
{
    VALUE cmd;

    cmd = rb_str_new2( "SELECT pg_stop_backup();");
    pgresult_clear( pg_statement_exec( self, cmd, Qnil));
    return Qnil;
}



/*
 * Document-class: Pg::Conn::ExecError
 *
 * Error while querying from a PostgreSQL connection.
 */


/*
 * Document-class: Pg::Conn::TransactionError
 *
 * Nested transaction blocks.  Use savepoints.
 */


/*
 * Document-class: Pg::Conn::CopyError
 *
 * Nested transaction blocks.  Use savepoints.
 */


void
Init_pgsql_conn_exec( void)
{

#ifdef RDOC_NEEDS_THIS
    rb_cPgConn = rb_define_class_under( rb_mPg, "Conn", rb_cObject);
#endif

    rb_ePgConnExec    = rb_define_class_under( rb_cPgConn, "ExecError",        rb_ePgError);
    rb_ePgConnTimeout = rb_define_class_under( rb_cPgConn, "Timeout",          rb_ePgError);
    rb_ePgConnTrans   = rb_define_class_under( rb_cPgConn, "TransactionError", rb_ePgError);
    rb_ePgConnCopy    = rb_define_class_under( rb_cPgConn, "CopyError",        rb_ePgError);

    rb_define_method( rb_cPgConn, "exec", &pgconn_exec, -1);
    rb_define_method( rb_cPgConn, "send", &pgconn_send, -1);
    rb_define_method( rb_cPgConn, "fetch", &pgconn_fetch, -1);
    rb_define_method( rb_cPgConn, "fetch_rows", &pgconn_fetch_rows, -1);

    rb_define_method( rb_cPgConn, "query", &pgconn_query, -1);
    rb_define_method( rb_cPgConn, "select_row", &pgconn_select_row, -1);
    rb_define_method( rb_cPgConn, "select_value", &pgconn_select_value, -1);
    rb_define_method( rb_cPgConn, "select_values", &pgconn_select_values, -1);
    rb_define_method( rb_cPgConn, "get_notify", &pgconn_get_notify, 0);


#define TRANS_DEF( c) rb_define_const( rb_cPgConn, "T_" #c, INT2FIX( PQTRANS_ ## c))
    TRANS_DEF( IDLE);
    TRANS_DEF( ACTIVE);
    TRANS_DEF( INTRANS);
    TRANS_DEF( INERROR);
    TRANS_DEF( UNKNOWN);
#undef TRANS_DEF

    rb_define_method( rb_cPgConn, "transaction", &pgconn_transaction, -1);
    rb_define_method( rb_cPgConn, "subtransaction", &pgconn_subtransaction, -1);
    rb_define_alias( rb_cPgConn, "savepoint", "subtransaction");
    rb_define_method( rb_cPgConn, "transaction_status", &pgconn_transaction_status, 0);


    rb_define_method( rb_cPgConn, "copy_stdin", &pgconn_copy_stdin, -1);
    rb_define_method( rb_cPgConn, "putline", &pgconn_putline, 1);
    rb_define_alias( rb_cPgConn, "put", "putline");
    rb_define_method( rb_cPgConn, "copy_stdout", &pgconn_copy_stdout, -1);
    rb_define_method( rb_cPgConn, "getline", &pgconn_getline, -1);
    rb_define_alias( rb_cPgConn, "get", "getline");
    rb_define_method( rb_cPgConn, "each_line", &pgconn_each_line, 0);

    rb_define_method( rb_cPgConn, "backup", &pgconn_backup, 1);

    id_to_a  = 0;
    id_fetch = 0;
}

