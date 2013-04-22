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

#ifdef HAVE_HEADER_POSTGRES_H
    #include <postgres.h>
#endif
#ifdef HAVE_HEADER_LIBPQ_FE_H
    #include <libpq-fe.h>
#endif
#ifdef HAVE_HEADER_CATALOG_PG_TYPE_H
    #include <catalog/pg_type.h>
#endif
#include "undef.h"


extern VALUE rb_mPg;
extern VALUE rb_ePgError;

extern void Init_pgsql( void);

#endif

