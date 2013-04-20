/*
 *  lo.h  --  Pg module access for large object support
 */

#ifndef __LO_H
#define __LO_H

#include "base.h"

struct pglarge_data
{
    PGconn *conn;
    Oid     lo_oid;
    int     lo_fd;
    char   *buf;
    int     len;
    int     rest;
};


extern void Init_pgsqllo( void);

#endif

