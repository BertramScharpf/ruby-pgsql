/*
 *  lo.c  --  Pg module access for large object support
 */

#include "lo.h"

#include <libpq/libpq-fs.h>


typedef struct pglarge_object
{
    PGconn *pgconn;
    Oid     lo_oid;
    int     lo_fd;
    char   *buf;
    int     len;
    int     rest;
} PGlarge;


static VALUE pgconn_loimport( VALUE self, VALUE filename);
static VALUE pgconn_loexport( VALUE self, VALUE lo_oid, VALUE filename);
static VALUE pgconn_lounlink( VALUE self, VALUE lo_oid);
static VALUE pgconn_locreate( int argc, VALUE *argv, VALUE self);
static VALUE pgconn_loopen( int argc, VALUE *argv, VALUE self);
static VALUE pgconn_losize( VALUE self, VALUE lo_oid);

static VALUE locreate_pgconn( PGconn *conn, VALUE nmode);
static VALUE loopen_pgconn(   PGconn *conn, VALUE nmode, VALUE objid);


static PGlarge *get_pglarge( VALUE obj);
static VALUE    loopen_int( PGconn *conn, int objid, int nmode);
static void     free_pglarge( PGlarge *ptr);
static int      large_read(  PGlarge *pglarge, char *buf, int len);
static int      large_tell(  PGlarge *pglarge);
static int      large_lseek( PGlarge *pglarge, int offset, int whence);
static VALUE    loread_all( VALUE self);
static VALUE    pglarge_new( PGconn *conn, Oid lo_oid, int lo_fd);
static void     freebuf_rewind( PGlarge *ptr, int warn);

static VALUE pglarge_oid( VALUE self);
static VALUE pglarge_close( VALUE self);
static VALUE pglarge_read( int argc, VALUE *argv, VALUE self);
static VALUE pglarge_each_line( VALUE self);
static VALUE pglarge_rewind( VALUE self);
static VALUE pglarge_write( VALUE self, VALUE buffer);
static VALUE pglarge_seek( VALUE self, VALUE offset, VALUE whence);
static VALUE pglarge_tell( VALUE self);
static VALUE pglarge_size( VALUE self);


static VALUE rb_cPgLarge;


/*
 * call-seq:
 *    conn.lo_import( file) -> oid
 *
 * Import a file to a large object.  Returns an oid on success.  On
 * failure, it raises a Pg::Error exception.
 */
VALUE
pgconn_loimport( self, filename)
    VALUE self, filename;
{
    Oid lo_oid;
    PGconn *conn;

    conn = get_pgconn( self);
    lo_oid = lo_import( conn, StringValueCStr( filename));
    if (lo_oid == 0)
        pg_raise_pgconn( conn);
    return INT2NUM( lo_oid);
}

/*
 * call-seq:
 *    conn.lo_export( oid, file )
 *
 * Saves a large object of _oid_ to a _file_.
 */
VALUE
pgconn_loexport( self, lo_oid, filename)
    VALUE self, lo_oid, filename;
{
    int oid;
    PGconn *conn;

    oid = NUM2INT( lo_oid);
    if (oid < 0)
        rb_raise( rb_ePgError, "invalid large object oid %d", oid);
    conn = get_pgconn( self);
    if (!lo_export( conn, oid, StringValueCStr( filename)))
        pg_raise_pgconn( conn);
    return Qnil;
}

/*
 * call-seq:
 *    conn.lo_unlink( oid)
 *
 * Unlinks (deletes) the postgres large object of _oid_.
 */
VALUE
pgconn_lounlink( self, lo_oid)
    VALUE self, lo_oid;
{
    PGconn *conn;
    int oid;

    oid = NUM2INT( lo_oid);
    if (oid < 0)
        rb_raise( rb_ePgError, "invalid oid %d", oid);
    conn = get_pgconn( self);
    if (lo_unlink( conn, oid) < 0)
        pg_raise_pgconn( conn);
    return Qnil;
}

/*
 * call-seq:
 *    conn.lo_create( [mode] ) -> Pg::Large
 *    conn.lo_create( [mode] ) { |pglarge| ... } -> oid
 *
 * Returns a Pg::Large instance on success.  On failure, it raises Pg::Error
 * exception. <i>(See #lo_open for information on _mode_.)</i>
 *
 * If a block is given, the blocks result is returned.
 *
 */
VALUE
pgconn_locreate( argc, argv, self)
    int argc;
    VALUE *argv;
    VALUE self;
{
    VALUE nmode;

    rb_scan_args( argc, argv, "01", &nmode);
    return locreate_pgconn( get_pgconn( self), nmode);
}

/*
 * call-seq:
 *    conn.lo_open( oid, [mode] ) -> Pg::Large
 *    conn.lo_open( oid, [mode] ) { |pglarge| ... } -> obj
 *
 * Open a large object of _oid_.  Returns a Pg::Large instance on success.
 * The _mode_ argument specifies the mode for the opened large object,
 * which is either +INV_READ+, or +INV_WRITE+.
 * * If _mode_ On failure, it raises a Pg::Error exception.
 * * If _mode_ is omitted, the default is +INV_READ+.
 *
 * If a block is given, the block's result is returned.
 *
 */
VALUE
pgconn_loopen( argc, argv, self)
    int argc;
    VALUE *argv;
    VALUE self;
{
    VALUE nmode, objid;

    rb_scan_args( argc, argv, "11", &objid, &nmode);
    return loopen_pgconn( get_pgconn( self), objid, nmode);
}

/*
 * call-seq:
 *    conn.lo_size( oid) -> num
 *
 * Determine the size of the large object in bytes.
 */
VALUE
pgconn_losize( self, lo_oid)
    VALUE self;
    VALUE lo_oid;
{
    PGconn *conn;
    int oid;
    int fd;
    int pos, end;
    int ret;

    conn = get_pgconn( self);
    oid = NUM2INT( lo_oid);
    fd = lo_open( conn, oid, INV_READ);
    if (fd < 0)
        pg_raise_pgconn( conn);
    pos = lo_tell( conn, fd);
    end = lo_lseek( conn, fd, 0, SEEK_END);
    ret = lo_close( conn, fd);
    if (pos < 0 || end < 0 || ret < 0)
        pg_raise_pgconn( conn);
    return INT2NUM( end);
}



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
        pg_raise_pgconn( conn);
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
    if (ptr->buf != NULL)
        xfree( ptr->buf);
    free( ptr);
}

int
large_read( pglarge, buf, len)
    PGlarge *pglarge;
    char *buf;
    int len;
{
    int siz;

    siz = lo_read( pglarge->pgconn, pglarge->lo_fd, buf, len);
    if (siz == -1)
        pg_raise_pgconn( pglarge->pgconn);
    return siz;
}

int
large_tell( pglarge)
    PGlarge *pglarge;
{
    int pos;

    pos = lo_tell( pglarge->pgconn, pglarge->lo_fd);
    if (pos == -1)
        pg_raise_pgconn( pglarge->pgconn);
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
        pg_raise_pgconn( pglarge->pgconn);
    return ret;
}

VALUE
loread_all( self)
    VALUE self;
{
    PGlarge *pglarge;
    VALUE str;
    long bytes = 0;
    int n;

    pglarge = get_pglarge( self);
    freebuf_rewind( pglarge, 1);
    str = rb_tainted_str_new( 0, 0);
    do {
        rb_str_resize( str, bytes + BUFSIZ);
        n = large_read( pglarge, RSTRING_PTR( str) + bytes, BUFSIZ);
        bytes += n;
    } while (n >= BUFSIZ);
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
    pglarge->buf = NULL;

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
        pg_raise_pgconn( conn);
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
            pg_raise_pgconn( pglarge->pgconn);
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
    PGlarge *pglarge;
    char *buf;
    int siz;
    VALUE length;

    rb_scan_args( argc, argv, "01", &length);
    if (NIL_P( length))
        return loread_all( self);

    len = NUM2INT( length);
    if (len < 0)
        rb_raise( rb_ePgError, "negative length %d given", len);

    pglarge = get_pglarge( self);
    freebuf_rewind( pglarge, 1);
    buf = ALLOCA_N( char, len);
    siz = large_read( pglarge, buf, len);
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
    PGlarge *q;
    VALUE line;
    int s;
    int j, l;
    char *p, *b;
    int nl;
#define EACH_LINE_BS BUFSIZ

    q = get_pglarge( self);
    RETURN_ENUMERATOR( self, 0, 0);
    line = rb_tainted_str_new( NULL, 0);
    /* The code below really looks weird but is thoroughly tested. */
    if (q->buf == NULL) {
        large_lseek( q, 0, SEEK_SET);
        q->buf = ALLOC_N( char, EACH_LINE_BS);
        q->rest = 0;
    } else
        p = q->buf + q->len - q->rest;
    for (;;) {
        if (q->rest == 0) {
            q->len = large_read( q, q->buf, EACH_LINE_BS);
            q->rest = q->len;
            p = q->buf;
        }
        if (q->len == 0)
            break;
        j = q->rest, b = p;
        do {
            j--;
            nl = *p++ == '\n';
        } while (!nl && j > 0);
        l = p - b;
        rb_str_cat( line, b, l);
        q->rest -= l;
        if (nl) {
            rb_yield( line);
            line = rb_tainted_str_new( NULL, 0);
        }
    }
    if (RSTRING_LEN( line) > 0)
        rb_yield( line);
    return Qnil;
#undef EACH_LINE_BS
}

/*
 * call-seq:
 *    lrg.rewind()     -> nil
 *
 * Rewind after an aborted +each_line+.
 */
VALUE
pglarge_rewind( self)
    VALUE self;
{
    freebuf_rewind( get_pglarge( self), 0);
    return Qnil;
}

void
freebuf_rewind( ptr, warn)
    PGlarge *ptr;
    int warn;
{
    int ret;

    if (ptr->buf != NULL) {
        if (warn)
            rb_warn( "Aborting each_line processing.");
        xfree( ptr->buf);
        ptr->buf = NULL;
        large_lseek( ptr, 0, SEEK_SET);
    }
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
    pglarge = get_pglarge( self);
    freebuf_rewind( pglarge, 1);
    n = lo_write( pglarge->pgconn, pglarge->lo_fd,
                  RSTRING_PTR( buffer), RSTRING_LEN( buffer));
    if (n == -1)
        pg_raise_pgconn( pglarge->pgconn);
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
    PGlarge *pglarge;

    pglarge = get_pglarge( self);
    freebuf_rewind( pglarge, 1);
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
    PGlarge *pglarge;
    int r;

    pglarge = get_pglarge( self);
    r = INT2NUM( large_tell( pglarge));
    if (pglarge->buf != NULL)
        r -= pglarge->len - pglarge->rest;
    return r;
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
 *
 * == Deprecation warning
 *
 * Be aware that TOAST, the PostgreSQL storage technique, handles
 * data fields to the size of 1GB, and compresses them automatically.
 * Normally, there is no need to implement anything by large objects.
 */

void
Init_pgsqllo( void)
{
    VALUE rb_cPgConn;

    rb_require( "pgsql");

    rb_mPg = rb_const_get( rb_cObject, rb_intern( "Pg"));
    rb_ePgError = rb_const_get( rb_mPg, rb_intern( "Error"));

    rb_cPgConn = rb_const_get( rb_mPg, rb_intern( "Conn"));
    rb_define_method( rb_cPgConn, "lo_import", pgconn_loimport, 1);
    rb_define_alias( rb_cPgConn, "loimport", "lo_import");
    rb_define_method( rb_cPgConn, "lo_export", pgconn_loexport, 2);
    rb_define_alias( rb_cPgConn, "loexport", "lo_export");
    rb_define_method( rb_cPgConn, "lo_unlink", pgconn_lounlink, 1);
    rb_define_alias( rb_cPgConn, "lounlink", "lo_unlink");
    rb_define_method( rb_cPgConn, "lo_create", pgconn_locreate, -1);
    rb_define_alias( rb_cPgConn, "locreate", "lo_create");
    rb_define_method( rb_cPgConn, "lo_open", pgconn_loopen, -1);
    rb_define_alias( rb_cPgConn, "loopen", "lo_open");
    rb_define_method( rb_cPgConn, "lo_size", pgconn_losize, 1);
    rb_define_alias( rb_cPgConn, "losize", "lo_size");

    rb_cPgLarge = rb_define_class_under( rb_mPg, "Large", rb_cObject);
    rb_define_method( rb_cPgLarge, "oid", pglarge_oid, 0);
    rb_define_method( rb_cPgLarge, "close", pglarge_close, 0);
    rb_define_method( rb_cPgLarge, "read", pglarge_read, -1);
    rb_define_method( rb_cPgLarge, "each_line", pglarge_each_line, 0);
    rb_define_alias( rb_cPgLarge, "eat_lines", "each_line");
    rb_define_method( rb_cPgLarge, "rewind", pglarge_rewind, 0);
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

