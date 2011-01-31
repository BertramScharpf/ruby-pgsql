/*
 *  row.h  --  Pg query rows
 */

#ifndef __ROW_H
#define __ROW_H

#include "module.h"


extern VALUE rb_cPgRow;

extern VALUE fetch_pgrow( PGresult *result, int row_num);
extern VALUE fetch_pgresult( PGresult *result, int row, int column);

extern void init_pg_row( void);

#endif

