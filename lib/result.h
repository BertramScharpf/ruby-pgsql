/*
 *  result.h  --  Pg query results
 */

#ifndef __RESULT_H
#define __RESULT_H

#include "module.h"

#include <libpq-fe.h>


extern VALUE rb_ePgResError;
extern VALUE rb_cPgResult;


extern void      pg_checkresult( PGconn *conn, PGresult *result);
extern VALUE     fetch_fields( PGresult *result);
extern PGresult *get_pgresult( VALUE obj);
extern VALUE     string_unescape_bytea( char *escaped);


extern VALUE pgresult_new( PGconn *conn, PGresult *ptr);
extern VALUE pgresult_clear( VALUE obj);

extern VALUE pgresult_each( VALUE self);


extern void init_pg_result( void);

#endif

