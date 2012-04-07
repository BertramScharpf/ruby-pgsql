/*
 *  lo.c  --  Pg module access for large object support
 *
 */

#include "lo.h"

#include "large.h"


void
Init_pgsqllo( void)
{
    rb_require( "pgsql");

    rb_mPg = rb_const_get( rb_cObject, rb_intern( "Pg"));
    rb_ePgError = rb_const_get( rb_mPg, rb_intern( "Error"));

    Init_pgsql_large();
}

