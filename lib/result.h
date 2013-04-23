/*
 *  result.h  --  Pg query results
 */

#ifndef __RESULT_H
#define __RESULT_H

#include "module.h"

struct pgresult_data {
    PGresult           *res;
    struct pgconn_data *conn;
};

extern VALUE rb_cBigDecimal;

extern VALUE pgreserror_new( PGresult *ptr, VALUE cmd, VALUE args);

extern PGresult *get_pgresult( VALUE obj);


extern VALUE pgresult_new( struct pgconn_data *conn, PGresult *ptr);
extern VALUE pgresult_clear( VALUE obj);


extern VALUE fetch_fields( PGresult *result);
extern VALUE field_index( VALUE fields, VALUE name);
extern VALUE fetch_pgresult( PGresult *result, int row, int column);
extern VALUE fetch_pgrow( PGresult *result, int row_num, VALUE fields);
extern VALUE pgresult_each( VALUE self);


extern void Init_pgsql_result( void);

#endif

