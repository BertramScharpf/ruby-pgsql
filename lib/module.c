/*
 *  module.c  --  Pg module
 *
 */

#include "module.h"

#include "conn.h"
#include "large.h"
#include "result.h"
#include "row.h"


#define PGSQL_VERSION "1.0"


ID id_new;


/********************************************************************
 *
 * Document-module: Pg
 *
 * The module to enclose everything.
 *
 * See the Pg::Conn class for information on opening a database.
 */

/********************************************************************
 *
 * Document-class: Pg::Error
 *
 * Generic PostgreSQL error.
 */

void
Init_pgsql( void)
{
    rb_mPg = rb_define_module( "Pg");

    rb_define_const( rb_mPg, "VERSION",
                                rb_obj_freeze( rb_str_new2( PGSQL_VERSION)));

    rb_ePgError = rb_define_class_under( rb_mPg, "Error", rb_eStandardError);

    id_new = rb_intern( "new");

    Init_pgsql_conn();
    Init_pgsql_result();
    Init_pgsql_row();
}

