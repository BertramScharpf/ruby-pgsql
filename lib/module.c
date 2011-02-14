/*
 *  module.c  --  Pg module
 *
 */

#include "module.h"

#include "conn.h"
#include "large.h"
#include "result.h"
#include "row.h"


ID id_new;

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

/********************************************************************
 *
 * Document-class: Pg::ExecError
 *
 * Error while communicating through a PostgreSQL connection.
 */

void
Init_pgsql( void)
{
    rb_mPg = rb_define_module( "Pg");

    rb_define_const( rb_mPg, "VERSION", rb_obj_freeze( rb_str_new2( "1.0")));

    rb_ePgError = rb_define_class_under( rb_mPg, "Error", rb_eStandardError);

    id_new = rb_intern( "new");

    Init_pgsql_conn();
    Init_pgsql_large();
    Init_pgsql_result();
    Init_pgsql_row();
}

