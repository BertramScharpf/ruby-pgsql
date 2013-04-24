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
    VALUE   command;
    VALUE   params;
    VALUE   notice;
};


extern VALUE rb_cPgConn;


extern void pg_check_conninvalid( struct pgconn_data *c);


extern void pgconn_clear( struct pgconn_data *c);
extern struct pgconn_data *get_pgconn( VALUE obj);

extern const char *pgconn_destring(  struct pgconn_data *ptr, VALUE str, int *len);
extern VALUE       pgconn_mkstring(  struct pgconn_data *ptr, const char *str);
extern VALUE       pgconn_mkstringn( struct pgconn_data *ptr, const char *str, int len);

extern void Init_pgsql_conn( void);


#endif

