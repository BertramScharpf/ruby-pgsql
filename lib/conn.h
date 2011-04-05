/*
 *  conn.h  --  PostgreSQL connection
 */

#ifndef __CONN_H
#define __CONN_H

#include "module.h"

extern int translate_results;

extern void pg_raise_pgconn( PGconn *conn);

extern void Init_pgsql_conn( void);

#endif

