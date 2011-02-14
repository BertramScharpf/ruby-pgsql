/*
 *  large.h  --  Pg large objects
 */

#ifndef __LARGE_H
#define __LARGE_H

#include "module.h"


extern VALUE locreate_pgconn( PGconn *conn, VALUE nmode);
extern VALUE loopen_pgconn(   PGconn *conn, VALUE nmode, VALUE objid);

extern void Init_pgsql_large( void);

#endif

