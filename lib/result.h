/*
 *  result.h  --  Pg query results
 */

#ifndef __RESULT_H
#define __RESULT_H

#include "module.h"

#include <libpq-fe.h>


extern VALUE rb_ePGResError;
extern VALUE rb_cPGResult;


extern void      pg_checkresult( PGconn *conn, PGresult *result);
extern VALUE     fetch_fields( PGresult *result);
extern PGresult *get_pgresult( VALUE obj);
extern VALUE     pgresult_result_with_clear( VALUE self);

/* TODO */
extern VALUE fetch_pgrow( PGresult *result, int row_num);
extern VALUE fetch_pgresult( PGresult *result, int row, int column);
/* Diese stehen noch in pgsql.c */




extern VALUE pgresult_new( PGconn *conn, PGresult *ptr);
extern VALUE pgresult_clear( VALUE obj);



extern void init_pg_result( void);

#endif

