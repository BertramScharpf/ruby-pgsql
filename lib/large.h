/*
 *  large.h  --  Pg large objects
 */

#ifndef __LARGE_H
#define __LARGE_H

#include "module.h"


extern VALUE rb_cPgLarge;


extern VALUE locreate_pgconn( PGconn *conn, VALUE nmode);
extern VALUE loopen_pgconn(   PGconn *conn, VALUE nmode, VALUE objid);


extern void init_pg_large( void);


#endif

