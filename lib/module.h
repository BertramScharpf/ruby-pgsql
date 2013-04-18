/*
 *  module.h  --  Pg module
 */

#ifndef __MODULE_H
#define __MODULE_H

#include "base.h"

#ifdef HAVE_HEADER_CATALOG_PG_TYPE_H
    #include <catalog/pg_type.h>
#endif


extern ID id_new;

extern void Init_pgsql( void);

#endif

