/*
 *  conn.h  --  Pg connection
 */

#ifndef __CONN_H
#define __CONN_H

#include "module.h"


extern VALUE rb_cPGConn;

extern int translate_results;

/*----------------------------------------------------------------*/
extern PGconn *get_pgconn( VALUE obj);
/*----------------------------------------------------------------*/

/*----------------------------------------------------------------*/
extern VALUE pgconn_trace( VALUE obj, VALUE port);
extern VALUE pgconn_untrace( VALUE obj);

extern VALUE pgconn_exec( int argc, VALUE *argv, VALUE obj);
extern VALUE pgconn_query( int argc, VALUE *argv, VALUE obj);
extern VALUE pgconn_select_one( int argc, VALUE *argv, VALUE self);
extern VALUE pgconn_select_value( int argc, VALUE *argv, VALUE self);
extern VALUE pgconn_select_values( int argc, VALUE *argv, VALUE self);
extern VALUE pgconn_async_exec( VALUE obj, VALUE str);
extern VALUE pgconn_async_query( VALUE obj, VALUE str);

extern VALUE pgconn_get_notify( VALUE obj);
extern VALUE pgconn_insert_table( VALUE obj, VALUE table, VALUE values);
extern VALUE pgconn_transaction( int argc, VALUE *argv, VALUE obj);
extern VALUE pgconn_subtransaction( int argc, VALUE *argv, VALUE obj);
extern VALUE pgconn_putline( VALUE obj, VALUE str);
extern VALUE pgconn_getline( VALUE obj);
extern VALUE pgconn_endcopy( VALUE obj);
extern VALUE pgconn_on_notice( VALUE self);
extern VALUE pgconn_transaction_status( VALUE obj);
extern VALUE pgconn_quote( VALUE obj, VALUE value);
extern VALUE pgconn_client_encoding( VALUE obj);
extern VALUE pgconn_set_client_encoding( VALUE obj, VALUE str);
/*----------------------------------------------------------------*/

extern void init_pg_conn( void);


#endif

