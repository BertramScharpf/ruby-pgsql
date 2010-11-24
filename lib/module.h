/*
 *  module.h  --  Pg module
 */

#ifndef __MODULE_H
#define __MODULE_H

#include <ruby.h>
#include <rubyio.h>
#include "undef.h"

#include <libpq-fe.h>


#define VERSION "1.2"


extern VALUE rb_mPg;
extern VALUE rb_ePGError;
extern VALUE rb_ePGExecError;
extern VALUE rb_ePGConnError;


extern void pg_raise_exec( PGconn *conn);
extern void pg_raise_conn( PGconn *conn);


extern void init_pg_module( void);


#endif

