/*
 *  base.h  --  Basic variables and functions
 */

#ifndef __BASE_H
#define __BASE_H

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
#include "undef.h"


extern VALUE rb_mPg;
extern VALUE rb_ePgError;

extern PGconn *get_pgconn( VALUE obj);

#endif

