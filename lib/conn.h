/*
 *  conn.h  --  PostgreSQL connection
 */

#ifndef __CONN_H
#define __CONN_H

#include "module.h"

struct pgconn_data {
    PGconn *conn;
#ifdef TODO_RUBY19_ENCODING
    rb_encoding *external;
    rb_encoding *internal;
#endif
    VALUE   notice;
};


extern VALUE rb_cPgConn;


extern void Init_pgsql_conn( void);


#ifdef TODO_DONE
extern int translate_results;

extern void pg_raise_pgconn( PGconn *conn);
#endif

#endif

