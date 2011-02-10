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


ID id_new;

VALUE rb_cBigDecimal;
VALUE rb_cDate;
VALUE rb_cDateTime;






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
    i = RARRAY_LEN( values);
    while (i--) {
        if (TYPE( RARRAY( RARRAY( values)->ptr[i])) != T_ARRAY) {
            rb_raise( rb_ePgError,
                     "second arg must contain some kind of arrays.");
        }
    }

    buffer = rb_str_new( 0, RSTRING_LEN( table) + 17 + 1);
    /* starts query */
    snprintf( RSTRING_PTR( buffer), RSTRING_LEN( buffer),
                "copy %s from stdin ", RSTRING_PTR( table));

    result = pg_pqexec( conn, RSTRING_PTR( buffer));
    PQclear( result);

    for (i = 0; i < RARRAY_LEN( values); i++) {
        /* sends data */
        PQputline( conn, RSTRING_PTR( buffer));
    }
    PQputline( conn, "\\.\n");
    res = PQendcopy( conn);

    return obj;
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
    PQputline( get_pgconn( obj), RSTRING_PTR( str));
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
        ret = PQgetline( conn, RSTRING_PTR( str) + bytes, size - bytes);
        switch (ret) {
        case EOF:
            return Qnil;
        case 0:
            rb_str_resize( str, strlen( RSTRING_PTR( str)));
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
        rb_raise( rb_ePgError, "cannot complete copying");
    }
    return Qnil;
}




void
Init_pgsql( void)
{
    rb_require( "bigdecimal");
    rb_cBigDecimal = rb_const_get( rb_cObject, rb_intern( "BigDecimal"));

    rb_require( "date");
    rb_require( "time");
    rb_cDate       = rb_const_get( rb_cObject, rb_intern( "Date"));
    rb_cDateTime   = rb_const_get( rb_cObject, rb_intern( "DateTime"));

    init_pg_module();
    init_pg_large();
    init_pg_row();
    init_pg_result();
    init_pg_conn();

    id_new   = rb_intern( "new");

}

