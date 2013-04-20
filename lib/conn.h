/*
 *  conn.h  --  PostgreSQL connection
 */

#ifndef __CONN_H
#define __CONN_H

#include "module.h"

struct pgconn_data {
    PGconn *conn;
};

extern int translate_results;

extern void pg_raise_pgconn( PGconn *conn);

extern PGconn *get_pgconn( VALUE obj);

extern void Init_pgsql_conn( void);

#endif

