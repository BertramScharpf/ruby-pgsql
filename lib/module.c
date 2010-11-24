/*
 *  module.c  --  Pg module
 */


#include "module.h"


VALUE rb_mPg;
VALUE rb_ePGError;

VALUE rb_ePGExecError;
VALUE rb_ePGConnError;


void pg_raise_exec( PGconn *conn)
{
    rb_raise( rb_ePGExecError, PQerrorMessage( conn));
}

void pg_raise_conn( PGconn *conn)
{
    rb_raise( rb_ePGConnError, PQerrorMessage( conn));
}

/* PostgreSQL API for Ruby */

/********************************************************************
 *
 * Document-module: Pg
 *
 * The module to enclose everything.
 *
 * See the Pg::Conn class for information on opening a database.
 */

void init_pg_module( void)
{
    rb_mPg = rb_define_module( "Pg");

    rb_define_const( rb_mPg, "VERSION", rb_obj_freeze( rb_str_new2( VERSION)));

    rb_ePGError = rb_define_class_under( rb_mPg, "Error", rb_eStandardError);

    rb_ePGExecError = rb_define_class_under( rb_mPg, "ExecError", rb_ePGError);
    rb_ePGConnError = rb_define_class_under( rb_mPg, "ConnError", rb_ePGError);
}

