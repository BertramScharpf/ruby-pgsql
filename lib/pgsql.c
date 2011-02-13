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

