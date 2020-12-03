/*
 *  module.c  --  Pg module
 */

#include "module.h"

#include "conn.h"
#include "result.h"


#define PGSQL_VERSION "1.6"


VALUE rb_mPg;
VALUE rb_ePgError;


/*
 * Document-module: Pg
 *
 * The module to enclose everything.
 *
 * See the Pg::Conn class for information on how to open a database
 * connection.
 */

/*
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

    Init_pgsql_conn();
    Init_pgsql_result();
}

