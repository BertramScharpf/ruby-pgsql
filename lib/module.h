/*
 *  module.h  --  Pg module
 */

#ifndef __MODULE_H
#define __MODULE_H


#if defined( HAVE_HEADER_RUBY_H)
    #include <ruby.h>
#elif defined( HAVE_HEADER_RUBY_RUBY_H)
    #include <ruby/ruby.h>
#endif
#if defined( HAVE_HEADER_RUBYIO_H)
    #include "rubyio.h"
#elif defined( HAVE_HEADER_RUBY_IO_H)
    #include "ruby/io.h"
#endif
#include "undef.h"

#include <postgres.h>
#include <libpq-fe.h>
#include <catalog/pg_type.h>
#include "undef.h"


extern VALUE rb_mPg;
extern VALUE rb_ePgError;

extern void Init_pgsql( void);

#endif

