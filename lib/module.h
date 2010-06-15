/*
 *  module.h  --  Pg module
 */

#ifndef __MODULE_H
#define __MODULE_H

#include <ruby.h>
#include <rubyio.h>
#include "undef.h"


#define VERSION "1.1"


extern VALUE rb_mPg;
extern VALUE rb_ePGError;


extern void init_pg_module( void);


#endif

