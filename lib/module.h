/*
 *  module.h  --  Pg module
 */

#ifndef __MODULE_H
#define __MODULE_H

#include <ruby.h>
#ifdef RUBY_VM
    #include "ruby/io.h"
#else
    #include "rubyio.h"
    #define rb_io_stdio_file GetWriteFile
#endif
#include "undef.h"

#include <postgres.h>
#include <libpq-fe.h>
#include <catalog/pg_type.h>
#include "undef.h"


extern ID id_new;

extern VALUE rb_mPg;
extern VALUE rb_ePgError;

extern void Init_pgsql( void);

#endif

