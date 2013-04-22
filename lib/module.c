/*
 *  module.c  --  Pg module
 */

#include "module.h"

#include "conn.h"
#ifdef TODO_DONE
#include "result.h"
#include "row.h"
#endif


#define PGSQL_VERSION "1.0"


VALUE rb_mPg;
VALUE rb_ePgError;


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

    Init_pgsql_conn();
#ifdef TODO_DONE
    Init_pgsql_result();
    Init_pgsql_row();
#endif
}

