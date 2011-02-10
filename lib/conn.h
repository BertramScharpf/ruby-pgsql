/*
 *  conn.h  --  Pg connection
 */

#ifndef __CONN_H
#define __CONN_H

#include "module.h"


extern VALUE rb_cPgConn;

extern int translate_results;

/*----------------------------------------------------------------*/
extern PGconn *get_pgconn( VALUE obj);
extern PGresult *pg_pqexec( PGconn *conn, const char *cmd);

extern VALUE pgconn_insert_table( VALUE obj, VALUE table, VALUE values);
extern VALUE pgconn_putline( VALUE obj, VALUE str);
extern VALUE pgconn_getline( VALUE obj);
extern VALUE pgconn_endcopy( VALUE obj);
/*----------------------------------------------------------------*/

extern void init_pg_conn( void);


#endif

