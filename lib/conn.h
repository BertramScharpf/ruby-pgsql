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
/*----------------------------------------------------------------*/

extern void init_pg_conn( void);


#endif

