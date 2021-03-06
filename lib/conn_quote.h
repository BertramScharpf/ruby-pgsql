/*
 *  conn_quote.h  --  PostgreSQL connection, string handling
 */

#ifndef __CONN_QUOTE_H
#define __CONN_QUOTE_H

#include "conn.h"


extern VALUE rb_cDate;
extern VALUE rb_cDateTime;


extern VALUE pg_monetary_class( void);


extern VALUE pgconn_stringize( VALUE self, VALUE obj);
extern VALUE pgconn_stringize_line( VALUE self, VALUE ary);


extern void Init_pgsql_conn_quote( void);


#endif

