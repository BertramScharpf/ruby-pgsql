/*
 *  conn_quote.h  --  PostgreSQL connection, string handling
 */

#ifndef __CONN_QUOTE_H
#define __CONN_QUOTE_H

#include "conn.h"


extern VALUE rb_cDate;
extern VALUE rb_cDateTime;
extern VALUE rb_cCurrency;


extern VALUE pg_currency_class( void);

extern VALUE string_unescape_bytea( char *escaped);


extern void Init_pgsql_conn_quote( void);


#endif

