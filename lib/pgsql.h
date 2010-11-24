/*
 *  pgsql.h  --  PGxxx classes
 */

#ifndef __PGSQL_H
#define __PGSQL_H

#include "module.h"

#include <postgres.h>
#include <catalog/pg_type.h>
#include "undef.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>



#define AssignCheckedStringValue(cstring, rstring) do { \
    if (!NIL_P(temp = rstring)) { \
        Check_Type(temp, T_STRING); \
        cstring = STR2CSTR(temp); \
    } \
} while (0)

#define rb_check_hash_type(x)   rb_check_convert_type(x, T_HASH,   "Hash",   "to_hash")

#define rb_define_singleton_alias(klass,new,old) rb_define_alias(rb_singleton_class(klass),new,old)

#define Data_Set_Struct(obj,ptr) do { \
    Check_Type(obj, T_DATA); \
    DATA_PTR(obj) = ptr; \
} while (0)

#define RUBY_CLASS(name) rb_const_get(rb_cObject, rb_intern(name))

#define SINGLE_QUOTE '\''

#endif

