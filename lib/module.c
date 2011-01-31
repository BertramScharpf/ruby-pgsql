/*
 *  module.c  --  Pg module
 */


#include "module.h"


VALUE rb_mPg;
VALUE rb_ePgError;

VALUE rb_ePgExecError;
VALUE rb_ePgConnError;


void pg_raise_exec( PGconn *conn)
{
    rb_raise( rb_ePgExecError, PQerrorMessage( conn));
}

void pg_raise_conn( PGconn *conn)
{
    rb_raise( rb_ePgConnError, PQerrorMessage( conn));
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

    rb_ePgError = rb_define_class_under( rb_mPg, "Error", rb_eStandardError);

    rb_ePgExecError = rb_define_class_under( rb_mPg, "ExecError", rb_ePgError);
    rb_ePgConnError = rb_define_class_under( rb_mPg, "ConnError", rb_ePgError);
}

