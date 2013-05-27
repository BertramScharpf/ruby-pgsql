/*
 *  result.h  --  Pg query results
 */

#ifndef __RESULT_H
#define __RESULT_H

#include "module.h"
#include "conn.h"


struct pgresult_data {
    PGresult           *res;
    struct pgconn_data *conn;
    VALUE               fields;
    VALUE               indices;
};



extern VALUE pgresult_new( PGresult *result, struct pgconn_data *conn, VALUE cmd, VALUE par);
extern VALUE pgresult_clear( VALUE self);
extern VALUE pgresult_each( VALUE self);
extern VALUE pg_fetchrow( struct pgresult_data *r, int num);
extern VALUE pg_fetchresult( struct pgresult_data *r, int row, int col);


extern void Init_pgsql_result( void);


#endif

