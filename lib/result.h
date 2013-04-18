/*
 *  result.h  --  Pg query results
 */

#ifndef __RESULT_H
#define __RESULT_H

#include "module.h"

extern VALUE rb_cBigDecimal;
extern VALUE rb_cDate;
extern VALUE rb_cDateTime;
extern VALUE rb_cCurrency;

extern VALUE pgreserror_new( PGresult *ptr, VALUE cmd, VALUE args);

extern VALUE     pg_currency_class( void);
extern int       pg_checkresult( PGresult *result);
extern PGresult *get_pgresult( VALUE obj);
extern VALUE     string_unescape_bytea( char *escaped);


extern VALUE pgresult_new( PGconn *conn, PGresult *ptr);
extern VALUE pgresult_clear( VALUE obj);


extern VALUE fetch_fields( PGresult *result);
extern VALUE field_index( VALUE fields, VALUE name);
extern VALUE fetch_pgresult( PGresult *result, int row, int column);
extern VALUE fetch_pgrow( PGresult *result, int row_num, VALUE fields);
extern VALUE pgresult_each( VALUE self);


extern void Init_pgsql_result( void);

#endif

