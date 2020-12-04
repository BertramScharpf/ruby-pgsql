/*
 *  result.c  --  Pg query results
 */


#include "result.h"

#include "conn_quote.h"


static void pgresult_init( struct pgresult_data *r, PGresult *result, struct pgconn_data *conn);
static VALUE pgreserror_new( VALUE result, VALUE cmd, VALUE par);

static struct pgresult_data *pgreserror_result( VALUE self);
static VALUE pgreserror_status(  VALUE self);
static VALUE pgreserror_sqlst(   VALUE self);
static VALUE pgreserror_primary( VALUE self);
static VALUE pgreserror_detail(  VALUE self);
static VALUE pgreserror_hint(    VALUE self);
static VALUE pgreserror_diag(    VALUE self, VALUE field);


static VALUE pgresult_s_translate_results_set( VALUE cls, VALUE fact);

static void pgresult_free( struct pgresult_data *ptr);
extern VALUE pgresult_new( PGresult *result, struct pgconn_data *conn, VALUE cmd, VALUE par);

extern VALUE pgresult_clear( VALUE self);

static VALUE pgresult_status( VALUE self);

static VALUE pgresult_fields( VALUE self);
static VALUE pgresult_field_indices( VALUE self);
static VALUE pgresult_num_fields( VALUE self);
static VALUE pgresult_fieldname( VALUE self, VALUE index);
static VALUE pgresult_fieldnum( VALUE self, VALUE name);

extern VALUE pgresult_each( VALUE self);
static VALUE pgresult_aref( int argc, VALUE *argv, VALUE self);
extern VALUE pg_fetchrow( struct pgresult_data *r, int num);
extern VALUE pg_fetchresult( struct pgresult_data *r, int row, int col);
static VALUE pgresult_num_tuples( VALUE self);

static VALUE pgresult_type( VALUE self, VALUE index);
static VALUE pgresult_size( VALUE self, VALUE index);
static VALUE pgresult_getvalue( VALUE self, VALUE row, VALUE col);
static VALUE pgresult_getlength( VALUE self, VALUE row, VALUE col);
static VALUE pgresult_getisnull( VALUE self, VALUE row, VALUE col);
static VALUE pgresult_getvalue_byname( VALUE self, VALUE row, VALUE field_name);

static VALUE pgresult_cmdtuples( VALUE self);
static VALUE pgresult_cmdstatus( VALUE self);
static VALUE pgresult_oid( VALUE self);


static VALUE rb_cPgResult;
static VALUE rb_ePgResError;

static ID id_new;
static ID id_parse;
static ID id_result;

static int translate_results = 1;




VALUE
pgreserror_new( VALUE result, VALUE cmd, VALUE par)
{
    struct pgresult_data *r;
    VALUE rse, msg;

    Data_Get_Struct( result, struct pgresult_data, r);
    msg = pgconn_mkstring( r->conn, PQresultErrorMessage( r->res));
    rse = rb_class_new_instance( 1, &msg, rb_ePgResError);
    rb_ivar_set( rse, id_result, result);
    rb_ivar_set( rse, rb_intern( "@command"),    cmd);
    rb_ivar_set( rse, rb_intern( "@parameters"), par);
    return rse;
}



static struct pgresult_data *
pgreserror_result( VALUE self)
{
    struct pgresult_data *r;

    Data_Get_Struct( rb_ivar_get( self, id_result), struct pgresult_data, r);
    return r;
}

/*
 * call-seq:
 *   pgqe.status() => num
 *
 * Forward PostgreSQL's error code.
 *
 */
VALUE
pgreserror_status( VALUE self)
{
    struct pgresult_data *r;

    r = pgreserror_result( self);
    return INT2FIX( PQresultStatus( r->res));
}

/*
 * call-seq:
 *   pgqe.sqlstate() => string
 *
 * Forward PostgreSQL's error code.
 *
 */
VALUE
pgreserror_sqlst( VALUE self)
{
    struct pgresult_data *r;

    r = pgreserror_result( self);
    return pgconn_mkstring( r->conn, PQresultErrorField( r->res, PG_DIAG_SQLSTATE));
}

/*
 * call-seq:
 *   pgqe.primary() => string
 *
 * Forward PostgreSQL's primary error message.
 *
 */
VALUE
pgreserror_primary( VALUE self)
{
    struct pgresult_data *r;

    r = pgreserror_result( self);
    return pgconn_mkstring( r->conn, PQresultErrorField( r->res, PG_DIAG_MESSAGE_PRIMARY));
}


/*
 * call-seq:
 *   pgqe.details() => string
 *
 * Forward PostgreSQL's error details.
 *
 */
VALUE
pgreserror_detail( VALUE self)
{
    struct pgresult_data *r;

    r = pgreserror_result( self);
    return pgconn_mkstring( r->conn, PQresultErrorField( r->res, PG_DIAG_MESSAGE_DETAIL));
}


/*
 * call-seq:
 *   pgqe.hint() => string
 *
 * Forward PostgreSQL's error hint.
 *
 */
VALUE
pgreserror_hint( VALUE self)
{
    struct pgresult_data *r;

    r = pgreserror_result( self);
    return pgconn_mkstring( r->conn, PQresultErrorField( r->res, PG_DIAG_MESSAGE_HINT));
}


/*
 * call-seq:
 *   pgqe.diag( field) => string
 *
 * Error diagnose message.  Give one of the PG_DIAG_* constants
 * to specify a field.
 *
 */
VALUE
pgreserror_diag( VALUE self, VALUE field)
{
    struct pgresult_data *r;

    r = pgreserror_result( self);
    return pgconn_mkstring( r->conn, PQresultErrorField( r->res, NUM2INT( field)));
}



/*
 * call-seq:
 *   Pg::Conn.translate_results = boolean
 *
 * When true (default), results are translated to an appropriate Ruby class.
 * When false, results are returned as +Strings+.
 *
 */
VALUE
pgresult_s_translate_results_set( VALUE cls, VALUE fact)
{
    translate_results = RTEST( fact) ? 1 : 0;
    return Qnil;
}



void
pgresult_free( struct pgresult_data *ptr)
{
    if (ptr->res != NULL)
        PQclear( ptr->res);
    free( ptr);
}

void
pgresult_init( struct pgresult_data *r, PGresult *result, struct pgconn_data *conn)
{
    r->res     = result;
    r->conn    = conn;
    r->fields  = Qnil;
    r->indices = Qnil;
}

VALUE
pgresult_new( PGresult *result, struct pgconn_data *conn, VALUE cmd, VALUE par)
{
    struct pgresult_data *r;
    VALUE res;

    res = Data_Make_Struct( rb_cPgResult, struct pgresult_data, 0, &pgresult_free, r);
    pgresult_init( r, result, conn);
    switch (PQresultStatus( result)) {
        case PGRES_EMPTY_QUERY:
        case PGRES_COMMAND_OK:
        case PGRES_TUPLES_OK:
        case PGRES_COPY_OUT:
        case PGRES_COPY_IN:
            break;
        case PGRES_BAD_RESPONSE:
        case PGRES_NONFATAL_ERROR:
        case PGRES_FATAL_ERROR:
            rb_exc_raise( pgreserror_new( res, cmd, par));
            break;
        default:
            rb_raise( rb_ePgError, "internal error: unknown result status.");
            break;
    }
    return res;
}

/*
 * call-seq:
 *    res.clear()
 *
 * Clears the Pg::Result object as the result of the query.
 */
VALUE
pgresult_clear( VALUE self)
{
    struct pgresult_data *r;

    Data_Get_Struct( self, struct pgresult_data, r);
    if (r->res != NULL) {
        PQclear( r->res);
        r->res = NULL;
    }
    return Qnil;
}


/*
 * call-seq:
 *    res.status()  -> int
 *
 * Returns the status of the query.  The status value is one of:
 *     * +EMPTY_QUERY+
 *     * +COMMAND_OK+
 *     * +TUPLES_OK+
 *     * +COPY_OUT+
 *     * +COPY_IN+
 */
VALUE
pgresult_status( VALUE self)
{
    struct pgresult_data *r;

    Data_Get_Struct( self, struct pgresult_data, r);
    return INT2FIX( PQresultStatus( r->res));
}



/*
 * call-seq:
 *    res.fields()
 *
 * Returns an array of Strings representing the names of the fields in the
 * result.
 *
 *   res = conn.exec( "SELECT foo, bar AS biggles, jim, jam FROM mytable")
 *   res.fields => [ 'foo', 'biggles', 'jim', 'jam']
 */
VALUE
pgresult_fields( VALUE self)
{
    struct pgresult_data *r;

    Data_Get_Struct( self, struct pgresult_data, r);
    if (NIL_P( r->fields)) {
        VALUE ary;
        int n, i;
        VALUE str;

        n = PQnfields( r->res);
        ary = rb_ary_new2( n);
        for (i = 0; n; i++, n--) {
            str = pgconn_mkstring( r->conn, PQfname( r->res, i));
            rb_str_freeze( str);
            rb_ary_push( ary, str);
        }
        rb_ary_freeze( ary);
        r->fields = ary;
    }
    return r->fields;
}

/*
 * call-seq:
 *    res.field_indices()
 *
 * Returns a hash that points to field numbers.
 * result.
 *
 *   res = conn.exec( "SELECT foo, bar AS biggles FROM mytable")
 *   res.field_indices => { 'foo' => 0, 'biggles' => 1 }
 */
VALUE
pgresult_field_indices( VALUE self)
{
    struct pgresult_data *r;

    Data_Get_Struct( self, struct pgresult_data, r);
    if (NIL_P( r->indices)) {
        VALUE hsh;
        int n, i;
        VALUE str;

        n = PQnfields( r->res);
        hsh = rb_hash_new();
        for (i = 0; n; i++, n--) {
            str = pgconn_mkstring( r->conn, PQfname( r->res, i));
            rb_str_freeze( str);
            rb_hash_aset( hsh, str, INT2FIX( i));
        }
        rb_hash_freeze( hsh);
        r->indices = hsh;
    }
    return r->indices;
}

/*
 * call-seq:
 *    res.num_fields()
 *
 * Returns the number of fields (columns) in the query result.
 *
 * Similar to <code>res.result[0].length</code> (but faster).
 */
VALUE
pgresult_num_fields( VALUE self)
{
    struct pgresult_data *r;

    Data_Get_Struct( self, struct pgresult_data, r);
    return INT2FIX( PQnfields( r->res));
}

/*
 * call-seq:
 *    res.fieldname( index)
 *
 * Returns the name of the field (column) corresponding to the index.
 *
 *   res = conn.exec "SELECT foo, bar AS biggles, jim, jam FROM mytable"
 *   res.fieldname 2     #=> 'jim'
 *   res.fieldname 1     #=> 'biggles'
 *
 * Equivalent to <code>res.fields[_index_]</code>.
 */
VALUE
pgresult_fieldname( VALUE self, VALUE index)
{
    struct pgresult_data *r;

    Data_Get_Struct( self, struct pgresult_data, r);
    return pgconn_mkstring( r->conn, PQfname( r->res, NUM2INT( index)));
}

/*
 * call-seq:
 *    res.fieldnum( name)
 *
 * Returns the index of the field specified by the string _name_.
 *
 *   res = conn.exec "SELECT foo, bar AS biggles, jim, jam FROM mytable"
 *   res.fieldnum 'foo'     #=> 0
 *
 * Raises an ArgumentError if the specified _name_ isn't one of the field
 * names; raises a TypeError if _name_ is not a String.
 */
VALUE
pgresult_fieldnum( VALUE self, VALUE name)
{
    struct pgresult_data *r;
    int n;

    StringValue( name);
    Data_Get_Struct( self, struct pgresult_data, r);
    n = PQfnumber( r->res, pgconn_destring( r->conn, name, NULL));
    if (n == -1)
        rb_raise( rb_eArgError, "Unknown field: %s", RSTRING_PTR( name));
    return INT2FIX( n);
}




/*
 * call-seq:
 *    res.each { |tuple| ... }  ->  nil or int
 *
 * Invokes the block for each tuple (row) in the result.
 *
 * Return the number of rows the query resulted in, or +nil+ if there
 * wasn't any (like <code>Numeric#nonzero?</code>).
 */
VALUE
pgresult_each( VALUE self)
{
    struct pgresult_data *r;
    int m, j;

    Data_Get_Struct( self, struct pgresult_data, r);
    for (j = 0, m = PQntuples( r->res); m; j++, m--)
        rb_yield( pg_fetchrow( r, j));
    return m ? INT2FIX( m) : Qnil;
}

/*
 * call-seq:
 *    res[ n]     -> ary
 *    res[ n, m]  -> obj
 *
 * Returns the tuple (row) corresponding to _n_.  Returns +nil+ if <code>_n_ >=
 * res.num_tuples</code>.
 *
 * Equivalent to <code>res.result[n]</code>.
 */
VALUE
pgresult_aref( int argc, VALUE *argv, VALUE self)
{
    struct pgresult_data *r;
    int a;
    VALUE aj, ai;
    int j, i;

    Data_Get_Struct( self, struct pgresult_data, r);
    a = rb_scan_args( argc, argv, "11", &aj, &ai);
    j = NUM2INT( aj);
    if (j < PQntuples( r->res)) {
        if (a == 1) {
            return pg_fetchrow( r, j);
        } else {
            if (TYPE( ai) == T_STRING) {
                ai = rb_hash_aref( pgresult_field_indices( self), ai);
                if (NIL_P( ai))
                    return Qnil;
            }
            i = NUM2INT( ai);
            if (i < PQnfields( r->res))
                return pg_fetchresult( r, j, i);
        }
    }
    return Qnil;
}


VALUE
pg_fetchrow( struct pgresult_data *r, int num)
{
    VALUE row;
    int n, i;

    n = PQnfields( r->res);
    if (num < PQntuples( r->res)) {
        row = rb_ary_new2( n);
        for (i = 0; n; ++i, --n)
            rb_ary_store( row, i, pg_fetchresult( r, num, i));
    } else
        row = Qnil;
    return row;
}

VALUE
pg_fetchresult( struct pgresult_data *r, int row, int col)
{
    char *string;
    Oid typ;
    VALUE cls, ret;

    if (PQgetisnull( r->res, row, col))
        return Qnil;

    string = PQgetvalue( r->res, row, col);
    if (string == NULL)
        return Qnil;

    if (!translate_results)
        return pgconn_mkstring( r->conn, string);

    typ = PQftype( r->res, col);
    cls = Qnil;
    ret = Qnil;
    switch (typ) {
    case NUMERICOID:
        {
            int typmod;

            typmod = PQfmod( r->res, col);
            if (typmod == -1 || (typmod - VARHDRSZ) & 0xffff) {
                ret = rb_funcall( Qnil, rb_intern( "BigDecimal"), 1, rb_str_new2( string));
                break;
            }
        }
        /* fall through if scale == 0 */
    case INT8OID:
    case INT4OID:
    case INT2OID:
    case OIDOID:
        ret = rb_cstr_to_inum( string, 10, 0);
        break;
    case FLOAT8OID:
    case FLOAT4OID:
        ret = rb_float_new( rb_cstr_to_dbl( string, Qfalse));
        break;
    case BOOLOID:
        ret = strchr( "tTyY", *string) != NULL ? Qtrue : Qfalse;
        break;
    case BYTEAOID:
        ret = rb_str_new2( string);
        break;
    case DATEOID:
        cls = rb_cDate;
        break;
    case TIMEOID:
    case TIMETZOID:
        cls = rb_cTime;
        break;
    case TIMESTAMPOID:
    case TIMESTAMPTZOID:
        cls = rb_cDateTime;
        break;
    case CASHOID:
        cls = pg_monetary_class();
        break;
    default:
        break;
    }
    if (NIL_P( ret)) {
        ret = pgconn_mkstring( r->conn, string);
        if (RTEST( cls))
            ret = rb_funcall( cls, id_parse, 1, ret);
    }
    return ret;
}

/*
 * call-seq:
 *    res.num_tuples()
 *
 * Returns the number of tuples (rows) in the query result.
 *
 * Similar to <code>res.rows.length</code> (but faster).
 */
VALUE
pgresult_num_tuples( VALUE self)
{
    struct pgresult_data *r;

    Data_Get_Struct( self, struct pgresult_data, r);
    return INT2FIX( PQntuples( r->res));
}



/*
 * call-seq:
 *    res.type( index)
 *
 * Returns the data type associated with the given column number.
 *
 * The integer returned is the internal +OID+ number (in PostgreSQL) of the
 * type.  If you have the PostgreSQL source available, you can see the OIDs for
 * every column type in the file <code>src/include/catalog/pg_type.h</code>.
 */
VALUE
pgresult_type( VALUE self, VALUE index)
{
    struct pgresult_data *r;
    int n;

    Data_Get_Struct( self, struct pgresult_data, r);
    n = PQftype( r->res, NUM2INT( index));
    return n ? INT2FIX( n) : Qnil;
}

/*
 * call-seq:
 *    res.size( index)
 *
 * Returns the size of the field type in bytes.  Returns <code>-1</code> if the
 * field is variable sized.
 *
 *   res = conn.exec "SELECT myInt, myVarChar50 FROM foo"
 *   res.size 0     #=> 4
 *   res.size 1     #=> -1
 */
VALUE
pgresult_size( VALUE self, VALUE index)
{
    struct pgresult_data *r;
    int n;

    Data_Get_Struct( self, struct pgresult_data, r);
    n = PQfsize( r->res, NUM2INT( index));
    return n ? INT2FIX( n) : Qnil;
}

/*
 * call-seq:
 *    res.value( row, col)
 *
 * Returns the value in tuple number <i>row</i>, field number
 * <i>col</i>. (Row <i>row</i>, column <i>col</i>.)
 *
 * Equivalent to <code>res.row[<i>row</i>][<i>col</i>]</code> (but
 * faster).
 */
VALUE
pgresult_getvalue( VALUE self, VALUE row, VALUE col)
{
    struct pgresult_data *r;

    Data_Get_Struct( self, struct pgresult_data, r);
    return pg_fetchresult( r, NUM2INT( row), NUM2INT( col));
}


/*
 * call-seq:
 *    res.getlength( row, col)  -> int
 *
 * Returns the (String) length of the field in bytes.
 *
 * Equivalent to
 * <code>res.value(<i>row</i>,<i>col</i>).length</code>.
 */
VALUE
pgresult_getlength( VALUE self, VALUE row, VALUE col)
{
    struct pgresult_data *r;

    Data_Get_Struct( self, struct pgresult_data, r);
    return INT2FIX( PQgetlength( r->res, NUM2INT( row), NUM2INT( col)));
}

/*
 * call-seq:
 *    res.getisnull( row, col) -> boolean
 *
 * Returns +true+ if the specified value is +nil+; +false+ otherwise.
 *
 * Equivalent to
 * <code>res.value(<i>row</i>,<i>col</i>)==+nil+</code>.
 */
VALUE
pgresult_getisnull( VALUE self, VALUE row, VALUE col)
{
    struct pgresult_data *r;

    Data_Get_Struct( self, struct pgresult_data, r);
    return PQgetisnull( r->res, NUM2INT( row), NUM2INT( col)) ? Qtrue : Qfalse;
}

/*
 * call-seq:
 *    res.value_byname( row, field_name )
 *
 * Returns the value in tuple number <i>row</i>, for the field named
 * <i>field_name</i>.
 *
 * Equivalent to (but faster than) either of:
 *
 *    res.row[<i>row</i>][ res.fieldnum(<i>field_name</i>) ]
 *    res.value( <i>row</i>, res.fieldnum(<i>field_name</i>) )
 *
 * <i>(This method internally calls #value as like the second example above;
 * it is slower than using the field index directly.)</i>
 */
VALUE
pgresult_getvalue_byname( VALUE self, VALUE row, VALUE field)
{
    return pgresult_getvalue( self, row, pgresult_fieldnum( self, field));
}



/*
 * call-seq:
 *    res.cmdtuples()
 *
 * Returns the number of tuples (rows) affected by the SQL command.
 *
 * If the SQL command that generated the Pg::Result was not one of +INSERT+,
 * +UPDATE+, +DELETE+, +MOVE+, or +FETCH+, or if no tuples (rows) were
 * affected, <code>0</code> is returned.
 */
VALUE
pgresult_cmdtuples( VALUE self)
{
    struct pgresult_data *r;
    char *n;

    Data_Get_Struct( self, struct pgresult_data, r);
    n = PQcmdTuples( r->res);
    return *n ? rb_cstr_to_inum( n, 10, 0) : Qnil;
}

/*
 * call-seq:
 *    res.cmdstatus()
 *
 * Returns the status string of the last query command.
 */
VALUE
pgresult_cmdstatus( VALUE self)
{
    struct pgresult_data *r;
    char *n;

    Data_Get_Struct( self, struct pgresult_data, r);
    n = PQcmdStatus( r->res);
    return n ? pgconn_mkstring( r->conn, n) : Qnil;
}

/*
 * call-seq:
 *    res.oid()  -> int
 *
 * Returns the +oid+.
 */
VALUE
pgresult_oid( VALUE self)
{
    struct pgresult_data *r;
    Oid n;

    Data_Get_Struct( self, struct pgresult_data, r);
    n = PQoidValue( r->res);
    return n == InvalidOid ? Qnil : INT2FIX( n);
}




/********************************************************************
 *
 * Document-class: Pg::Result
 *
 * The class to represent the query result tuples (rows).
 * An instance of this class is created as the result of every query.
 * You may need to invoke the #clear method of the instance when finished with
 * the result for better memory performance.
 */

/********************************************************************
 *
 * Document-class: Pg::Result::Error
 *
 * The information in a result that is an error.
 */

void
Init_pgsql_result( void)
{
    rb_cPgResult = rb_define_class_under( rb_mPg, "Result", rb_cObject);


    rb_ePgResError = rb_define_class_under( rb_cPgResult, "Error", rb_ePgError);
    rb_undef_method( CLASS_OF( rb_ePgResError), "new");

    rb_define_attr( rb_ePgResError, "command",    1, 0);
    rb_define_attr( rb_ePgResError, "parameters", 1, 0);

    rb_define_method( rb_ePgResError, "status", &pgreserror_status, 0);
    rb_define_method( rb_ePgResError, "sqlstate", &pgreserror_sqlst, 0);
    rb_define_alias( rb_ePgResError, "errcode", "sqlstate");
    rb_define_method( rb_ePgResError, "primary", &pgreserror_primary, 0);
    rb_define_method( rb_ePgResError, "details", &pgreserror_detail, 0);
    rb_define_method( rb_ePgResError, "hint", &pgreserror_hint, 0);

    rb_define_method( rb_ePgResError, "diag", &pgreserror_diag, 1);

#define PGD_DEF( c) rb_define_const( rb_ePgResError, #c, INT2FIX( PG_DIAG_ ## c))
    PGD_DEF( SEVERITY);
    PGD_DEF( SQLSTATE);
    PGD_DEF( MESSAGE_PRIMARY);
    PGD_DEF( MESSAGE_DETAIL);
    PGD_DEF( MESSAGE_HINT);
    PGD_DEF( STATEMENT_POSITION);
    PGD_DEF( INTERNAL_POSITION);
    PGD_DEF( INTERNAL_QUERY);
    PGD_DEF( CONTEXT);
    PGD_DEF( SOURCE_FILE);
    PGD_DEF( SOURCE_LINE);
    PGD_DEF( SOURCE_FUNCTION);
#undef PGD_DEF

    rb_define_singleton_method( rb_cPgResult, "translate_results=", pgresult_s_translate_results_set, 1);

    rb_undef_method( CLASS_OF( rb_cPgResult), "new");
    rb_define_method( rb_cPgResult, "clear", &pgresult_clear, 0);
    rb_define_alias( rb_cPgResult, "close", "clear");

    rb_define_method( rb_cPgResult, "status", &pgresult_status, 0);

    rb_define_method( rb_cPgResult, "fields", &pgresult_fields, 0);
    rb_define_method( rb_cPgResult, "field_indices", &pgresult_field_indices, 0);
    rb_define_alias( rb_cPgResult, "indices", "field_indices");
    rb_define_method( rb_cPgResult, "num_fields", &pgresult_num_fields, 0);
    rb_define_method( rb_cPgResult, "fieldname", &pgresult_fieldname, 1);
    rb_define_method( rb_cPgResult, "fieldnum", &pgresult_fieldnum, 1);

    rb_define_method( rb_cPgResult, "each", &pgresult_each, 0);
    rb_include_module( rb_cPgResult, rb_mEnumerable);
    rb_define_alias( rb_cPgResult, "rows", "entries");
    rb_define_alias( rb_cPgResult, "result", "entries");
    rb_define_method( rb_cPgResult, "[]", &pgresult_aref, -1);
    rb_define_method( rb_cPgResult, "num_tuples", &pgresult_num_tuples, 0);

    rb_define_method( rb_cPgResult, "type", &pgresult_type, 1);
    rb_define_method( rb_cPgResult, "size", &pgresult_size, 1);
    rb_define_method( rb_cPgResult, "getvalue", &pgresult_getvalue, 2);
    rb_define_method( rb_cPgResult, "getlength", &pgresult_getlength, 2);
    rb_define_method( rb_cPgResult, "getisnull", &pgresult_getisnull, 2);
    rb_define_method( rb_cPgResult, "getvalue_byname", &pgresult_getvalue_byname, 2);

#define RESC_DEF( c) rb_define_const( rb_cPgResult, #c, INT2FIX( PGRES_ ## c))
    RESC_DEF( EMPTY_QUERY);
    RESC_DEF( COMMAND_OK);
    RESC_DEF( TUPLES_OK);
    RESC_DEF( COPY_OUT);
    RESC_DEF( COPY_IN);
    RESC_DEF( BAD_RESPONSE);
    RESC_DEF( NONFATAL_ERROR);
    RESC_DEF( FATAL_ERROR);
#undef RESC_DEF

    rb_define_method( rb_cPgResult, "cmdtuples", &pgresult_cmdtuples, 0);
    rb_define_method( rb_cPgResult, "cmdstatus", &pgresult_cmdstatus, 0);
    rb_define_method( rb_cPgResult, "oid", &pgresult_oid, 0);


    id_new    = rb_intern( "new");
    id_parse  = rb_intern( "parse");
    id_result = rb_intern( "result");
}

