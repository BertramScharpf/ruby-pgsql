/*
 *  conn_exec.c  --  PostgreSQL connection, command execution
 */


#include "conn_exec.h"

#include "conn_quote.h"
#include "result.h"


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


static VALUE rb_ePgConnExec;



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
    if (NIL_P( par))
        result = PQexec( c->conn, RSTRING_PTR( cmd));
    else {
        char **v;
        int len;

        v = params_to_strings( conn, par, &len);
        result = PQexecParams( c->conn, RSTRING_PTR( cmd), len,
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
    if (NIL_P( par))
        res = PQsendQuery( c->conn, RSTRING_PTR( cmd));
    else {
        char **v;
        int len;

        v = params_to_strings( conn, par, &len);
        res = PQsendQueryParams( c->conn, RSTRING_PTR( cmd), len,
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
    VALUE *ptr;
    int l;
    char **values, **v;
    VALUE str;
    char *a;

    ptr = RARRAY_PTR( params);
    *len = l = RARRAY_LEN( params);
    values = ALLOC_N( char *, l);
    for (v = values; l; v++, ptr++, l--)
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
    r->conn->command = Qnil;
    r->conn->params  = Qnil;
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
    c->command = Qnil;
    c->params  = Qnil;
    return Qnil;
}






void
Init_pgsql_conn_exec( void)
{

#define ERR_DEF( n)  rb_define_class_under( rb_cPgConn, n, rb_ePgError)
    rb_ePgConnExec    = ERR_DEF( "Exec");
#undef ERR_DEF

    rb_define_method( rb_cPgConn, "exec", pgconn_exec, -1);
    rb_define_method( rb_cPgConn, "send", pgconn_send, -1);
    rb_define_method( rb_cPgConn, "fetch", pgconn_fetch, 0);

}

