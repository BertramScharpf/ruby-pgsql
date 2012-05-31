/*
 *  base.c  --  Basic variables and functions
 */

#include "base.h"

VALUE rb_mPg;
VALUE rb_ePgError;

PGconn *
get_pgconn( obj)
    VALUE obj;
{
    PGconn *conn;

    Data_Get_Struct( obj, PGconn, conn);
    if (conn == NULL)
        rb_raise( rb_ePgError, "not a valid connection");
    return conn;
}

