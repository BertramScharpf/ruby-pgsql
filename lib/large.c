/*
 *  large.c  --  Pg large objects
 *
 */

#include "large.h"

#include <libpq/libpq-fs.h>

typedef struct pglarge_object
{
    PGconn *pgconn;
    Oid     lo_oid;
    int     lo_fd;
} PGlarge;



static PGlarge *get_pglarge( VALUE obj);
static VALUE    loopen_int( PGconn *conn, int objid, int nmode);
static void     free_pglarge( PGlarge *ptr);
static int      large_tell(  PGlarge *pglarge);
static int      large_lseek( PGlarge *pglarge, int offset, int whence);
static VALUE    loread_all( VALUE obj);
static VALUE    pglarge_new( PGconn *conn, Oid lo_oid, int lo_fd);

static VALUE pglarge_oid( VALUE self);
static VALUE pglarge_close( VALUE self);
static VALUE pglarge_read( int argc, VALUE *argv, VALUE self);
static VALUE pglarge_each_line( VALUE self);
static VALUE pglarge_write( VALUE self, VALUE buffer);
static VALUE pglarge_seek( VALUE self, VALUE offset, VALUE whence);
static VALUE pglarge_tell( VALUE self);
static VALUE pglarge_size( VALUE self);


static VALUE rb_cPgLarge;



PGlarge *
get_pglarge( obj)
    VALUE obj;
{
    PGlarge *pglarge;

    Data_Get_Struct( obj, PGlarge, pglarge);
    return pglarge;
}

VALUE
loopen_int( conn, lo_oid, mode)
    PGconn *conn;
    int lo_oid;
    int mode;
{
    int fd;
    VALUE lob;

    fd = lo_open( conn, lo_oid, mode);
    if (fd < 0)
        rb_raise( rb_ePgError, "can't open large object");
    lob = pglarge_new( conn, lo_oid, fd);
    return rb_block_given_p() ?
        rb_ensure( rb_yield, lob, pglarge_close, lob) : lob;
}

void
free_pglarge( ptr)
    PGlarge *ptr;
{
    if (ptr->lo_fd > 0)
        lo_close( ptr->pgconn, ptr->lo_fd);
    free( ptr);
}

int
large_tell( pglarge)
    PGlarge *pglarge;
{
    int pos;

    pos = lo_tell( pglarge->pgconn, pglarge->lo_fd);
    if (pos == -1)
        rb_raise( rb_ePgError, "error while getting position");
    return pos;
}

int
large_lseek( pglarge, offset, whence)
    PGlarge *pglarge;
    int offset, whence;
{
    int ret;

    ret = lo_lseek( pglarge->pgconn, pglarge->lo_fd, offset, whence);
    if (ret == -1)
        rb_raise( rb_ePgError, "error while moving cursor");
    return ret;
}

VALUE
loread_all( obj)
    VALUE obj;
{
    PGlarge *pglarge;
    VALUE str;
    long bytes = 0;
    int n;

    pglarge = get_pglarge( obj);
    str = rb_tainted_str_new( 0, 0);
    for (;;) {
        rb_str_resize( str, bytes + BUFSIZ);
        n = lo_read( pglarge->pgconn, pglarge->lo_fd,
                     RSTRING_PTR( str) + bytes, BUFSIZ);
        bytes += n;
        if (n < BUFSIZ)
            break;
    }
    rb_str_resize( str, bytes);
    return str;
}

VALUE
pglarge_new( conn, lo_oid, lo_fd)
    PGconn *conn;
    Oid lo_oid;
    int lo_fd;
{
    VALUE obj;
    PGlarge *pglarge;

    obj = Data_Make_Struct( rb_cPgLarge, PGlarge, 0, free_pglarge, pglarge);
    pglarge->pgconn = conn;
    pglarge->lo_oid = lo_oid;
    pglarge->lo_fd = lo_fd;

    return obj;
}

VALUE
locreate_pgconn( conn, nmode)
    PGconn *conn;
    VALUE nmode;
{
    int mode;
    Oid lo_oid;

    mode = NIL_P( nmode) ? INV_WRITE : FIX2INT( nmode);
    lo_oid = lo_creat( conn, mode);
    if (lo_oid == 0)
        rb_raise( rb_ePgError, "can't creat large object");
    return loopen_int( conn, lo_oid, mode);
}

VALUE
loopen_pgconn( conn, objid, nmode)
    PGconn *conn;
    VALUE objid;
    VALUE nmode;
{
    Oid lo_oid;
    int mode;

    lo_oid = NUM2INT( objid);
    mode = NIL_P( nmode) ? INV_READ : FIX2INT( nmode);
    return loopen_int( conn, lo_oid, mode);
}



/*
 * call-seq:
 *    lrg.oid()
 *
 * Returns the large object's +oid+.
 */
VALUE
pglarge_oid( self)
    VALUE self;
{
    PGlarge *pglarge;

    pglarge = get_pglarge( self);
    return INT2NUM( pglarge->lo_oid);
}

/*
 * call-seq:
 *    lrg.close()
 *
 * Closes a large object.
 */
VALUE
pglarge_close( self)
    VALUE self;
{
    PGlarge *pglarge;
    int ret;

    pglarge = get_pglarge( self);
    if (pglarge != NULL) {
        ret = lo_close( pglarge->pgconn, pglarge->lo_fd);
        if (ret < 0 &&
                PQtransactionStatus( pglarge->pgconn) != PQTRANS_INERROR) {
            rb_raise( rb_ePgError, "cannot close large object");
        }
        DATA_PTR( self) = NULL;
    }
    return Qnil;
}


/*
 * call-seq:
 *    lrg.read( [length])
 *
 * Attempts to read _length_ bytes from large object.
 * If no _length_ is given, reads all data.
 */
VALUE
pglarge_read( argc, argv, self)
    int argc;
    VALUE *argv;
    VALUE self;
{
    int len;
    PGlarge *pglarge = get_pglarge( self);
    char *buf;
    int siz;

    VALUE length;
    rb_scan_args( argc, argv, "01", &length);
    if (NIL_P( length))
        return loread_all( self);

    len = NUM2INT( length);
    if (len < 0)
        rb_raise( rb_ePgError, "negative length %d given", len);

    buf = ALLOCA_N( char, len);
    siz = lo_read( pglarge->pgconn, pglarge->lo_fd, buf, len);
    if (siz < 0)
        rb_raise( rb_ePgError, "error while reading");
    return siz == 0 ? Qnil : rb_tainted_str_new( buf, siz);
}

/*
 * call-seq:
 *    lrg.each_line() { |line| ... }  -> nil
 *
 * Reads a large object line by line.
 */
VALUE
pglarge_each_line( self)
    VALUE self;
{
    PGlarge *pglarge;
    VALUE line;
    int len, i, j, s;
    char buf[ BUFSIZ], *p, *b, c;
    int ct;

    pglarge = get_pglarge( self);
    RETURN_ENUMERATOR( self, 0, 0);
    /* The code below really looks weird but is thoroughly tested. */
    line = rb_tainted_str_new( NULL, 0);
    do {
        len = lo_read( pglarge->pgconn, pglarge->lo_fd, buf, BUFSIZ);
        for (i = 0, j = len, p = buf; j > 0;) {
            s = i, b = buf + i;
            do
                i++, j--;
            while ((ct = *p++ != '\n') && j > 0);
            rb_str_cat( line, b, i - s);
            if (!ct) {
                rb_yield( line);
                line = rb_tainted_str_new( NULL, 0);
            }
        }
    } while (len == BUFSIZ);
    if (RSTRING_LEN(line) > 0)
        rb_yield( line);
    return Qnil;
}

/*
 * call-seq:
 *    lrg.write( str)
 *
 * Writes the string _str_ to the large object.
 * Returns the number of bytes written.
 */
VALUE
pglarge_write( self, buffer)
    VALUE self, buffer;
{
    PGlarge *pglarge;
    int n;

    StringValue( buffer);
    if (RSTRING_LEN( buffer) < 0)
        rb_raise( rb_ePgError, "write buffer zero string");

    pglarge = get_pglarge( self);
    n = lo_write( pglarge->pgconn, pglarge->lo_fd,
                  RSTRING_PTR( buffer), RSTRING_LEN( buffer));
    if (n == -1)
        rb_raise( rb_ePgError, "buffer truncated during write");

    return INT2FIX( n);
}

/*
 * call-seq:
 *    lrg.seek( offset, whence )
 *
 * Move the large object pointer to the _offset_.
 * Valid values for _whence_ are +SEEK_SET+, +SEEK_CUR+, and +SEEK_END+.
 * (Or 0, 1, or 2.)
 */
VALUE
pglarge_seek( self, offset, whence)
    VALUE self, offset, whence;
{
    PGlarge *pglarge = get_pglarge( self);
    return INT2NUM( large_lseek( pglarge, NUM2INT( offset), NUM2INT( whence)));
}

/*
 * call-seq:
 *    lrg.tell()
 *
 * Returns the current position of the large object pointer.
 */
VALUE
pglarge_tell( self)
    VALUE self;
{
    return INT2NUM( large_tell( get_pglarge( self)));
}

/*
 * call-seq:
 *    lrg.size()
 *
 * Returns the size of the large object.
 */
VALUE
pglarge_size( self)
    VALUE self;
{
    PGlarge *pglarge = get_pglarge( self);
    int start, end;

    start = large_tell( pglarge);
    end = large_lseek( pglarge, 0, SEEK_END);
    large_lseek( pglarge, start, SEEK_SET);
    return INT2NUM( end);
}


/********************************************************************
 *
 * Document-class: Pg::Large
 *
 * The class to access large objects.
 * An instance of this class is created as the result of
 * Pg::Conn#lo_import, Pg::Conn#lo_create, and Pg::Conn#lo_open.
 */

void
Init_pgsql_large( void)
{
#if 0
    rb_mPg = rb_define_module( "Pg");
#endif

    rb_cPgLarge = rb_define_class_under( rb_mPg, "Large", rb_cObject);
    rb_define_method( rb_cPgLarge, "oid", pglarge_oid, 0);
    rb_define_method( rb_cPgLarge, "close", pglarge_close, 0);
    rb_define_method( rb_cPgLarge, "read", pglarge_read, -1);
    rb_define_method( rb_cPgLarge, "each_line", pglarge_each_line, 0);
    rb_define_method( rb_cPgLarge, "write", pglarge_write, 1);
    rb_define_method( rb_cPgLarge, "seek", pglarge_seek, 2);
    rb_define_method( rb_cPgLarge, "tell", pglarge_tell, 0);
    rb_define_method( rb_cPgLarge, "size", pglarge_size, 0);

#define LRGC_DEF( c) rb_define_const( rb_cPgLarge, #c, INT2FIX( c))
    LRGC_DEF( INV_WRITE);
    LRGC_DEF( INV_READ);
    LRGC_DEF( SEEK_SET);
    LRGC_DEF( SEEK_CUR);
    LRGC_DEF( SEEK_END);
#undef LRGC_DEF
}

