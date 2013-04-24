/*
 *  conn_exec.c  --  PostgreSQL connection, command execution
 */


#include "conn_exec.h"

#include "conn_quote.h"


static PGresult *pg_statement_exec( VALUE conn, VALUE cmd, VALUE par);
static void      pg_statement_send( VALUE conn, VALUE cmd, VALUE par);
static char **params_to_strings( VALUE conn, VALUE params, int *len);
static void free_strings( char **strs, int len);
static void pg_parse_parameters( int argc, VALUE *argv, VALUE *cmd, VALUE *par);



static VALUE rb_ePgConnExec;




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
        rb_raise( rb_ePgConnExec, PQerrorMessage( c->conn));
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
        rb_raise( rb_ePgConnExec, PQerrorMessage( c->conn));
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





void
Init_pgsql_conn_exec( void)
{

#define ERR_DEF( n)  rb_define_class_under( rb_cPgConn, n, rb_ePgError)
    rb_ePgConnExec    = ERR_DEF( "Exec");
#undef ERR_DEF

}

