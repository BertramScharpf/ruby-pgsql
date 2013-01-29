/*
 *  result.c  --  Pg query results
 */


#include "result.h"

#include <st.h>
#include <intern.h>

#include "row.h"
#include "conn.h"


VALUE rb_cBigDecimal;
VALUE rb_cDate;
VALUE rb_cDateTime;
VALUE rb_cCurrency;

static ID id_parse;
static ID id_index;
static ID id_currency;

static int  get_field_number( PGresult *result, VALUE index);
static int  get_tuple_number( PGresult *result, VALUE index);

static VALUE pgresult_alloc( VALUE cls);
static VALUE pgresult_status( VALUE obj);
static VALUE pgresult_aref( int argc, VALUE *argv, VALUE obj);
static VALUE pgresult_fields( VALUE obj);
static VALUE pgresult_num_tuples( VALUE obj);
static VALUE pgresult_num_fields( VALUE obj);
static VALUE pgresult_fieldname( VALUE obj, VALUE index);
static VALUE pgresult_fieldnum( VALUE obj, VALUE name);
static VALUE pgresult_type( VALUE obj, VALUE index);
static VALUE pgresult_size( VALUE obj, VALUE index);
static VALUE pgresult_getvalue( VALUE obj, VALUE tup_num, VALUE field_num);
static VALUE pgresult_getvalue_byname( VALUE obj, VALUE tup_num,
                                                          VALUE field_name);
static VALUE pgresult_getlength( VALUE obj, VALUE tup_num, VALUE field_num);
static VALUE pgresult_getisnull( VALUE obj, VALUE tup_num, VALUE field_num);
static VALUE pgresult_cmdtuples( VALUE obj);
static VALUE pgresult_cmdstatus( VALUE obj);
static VALUE pgresult_oid( VALUE obj);


static VALUE pgreserror_status( VALUE obj);
static VALUE pgreserror_sqlst( VALUE self);
static VALUE pgreserror_primary( VALUE self);
static VALUE pgreserror_detail( VALUE self);
static VALUE pgreserror_hint( VALUE self);


static VALUE rb_cPgResult;
static VALUE rb_ePgResError;


VALUE
pg_currency_class( void)
{
    if (NIL_P( rb_cCurrency) && rb_const_defined( rb_cObject, id_currency))
        rb_cCurrency = rb_const_get( rb_cObject, id_currency);
    return rb_cCurrency;
}


int
pg_checkresult( result)
    PGresult *result;
{
    int s;

    s = PQresultStatus( result);
    switch (s) {
        case PGRES_EMPTY_QUERY:
        case PGRES_COMMAND_OK:
        case PGRES_TUPLES_OK:
        case PGRES_COPY_OUT:
        case PGRES_COPY_IN:
            break;
        case PGRES_BAD_RESPONSE:
        case PGRES_NONFATAL_ERROR:
        case PGRES_FATAL_ERROR:
            return -1;
            break;
        default:
            PQclear( result);
            rb_raise( rb_ePgError, "internal error: unknown result status.");
            break;
    }
    return s;
}

PGresult *
get_pgresult( obj)
    VALUE obj;
{
    PGresult *result;

    Data_Get_Struct( obj, PGresult, result);
    if (result == NULL)
        rb_raise( rb_ePgError, "query not performed");
    return result;
}

int
get_tuple_number( result, index)
    PGresult *result;
    VALUE index;
{
    int i;

    i = NUM2INT( index);
    if (i < 0 || i >= PQntuples( result))
        rb_raise( rb_eArgError, "invalid tuple number %d", i);
    return i;
}

int
get_field_number( result, index)
    PGresult *result;
    VALUE index;
{
    int i;

    i = NUM2INT( index);
    if (i < 0 || i >= PQnfields( result))
        rb_raise( rb_eArgError, "invalid field number %d", i);
    return i;
}



VALUE
pgreserror_new( result, cmd, args)
    PGresult *result;
    VALUE cmd;
    VALUE args;
{
    VALUE rse, msg;

    rse = pgresult_alloc( rb_ePgResError);
    DATA_PTR( rse) = result;
    msg = rb_str_new2( PQresultErrorMessage( result));
    rb_obj_call_init( rse, 1, &msg);
    rb_ivar_set( rse, rb_intern( "@command"),    cmd);
    rb_ivar_set( rse, rb_intern( "@parameters"), args);
    return rse;
}


/*
 * call-seq:
 *   pgqe.status() => num
 *
 * Forward PostgreSQL's error code.
 *
 */
VALUE
pgreserror_status( self)
    VALUE self;
{
    return INT2NUM( PQresultStatus( get_pgresult( self)));
}

/*
 * call-seq:
 *   pgqe.sqlstate() => string
 *
 * Forward PostgreSQL's error code.
 *
 */
VALUE
pgreserror_sqlst( self)
    VALUE self;
{
    char *e;

    e = PQresultErrorField( get_pgresult( self), PG_DIAG_SQLSTATE);
    return rb_str_new2( e);
}

/*
 * call-seq:
 *   pgqe.primary() => string
 *
 * Forward PostgreSQL's primary error message.
 *
 */
VALUE
pgreserror_primary( self)
    VALUE self;
{
    char *e;

    e = PQresultErrorField( get_pgresult( self), PG_DIAG_MESSAGE_PRIMARY);
    return rb_str_new2( e);
}


/*
 * call-seq:
 *   pgqe.details() => string
 *
 * Forward PostgreSQL's error details.
 *
 */
VALUE
pgreserror_detail( self)
    VALUE self;
{
    char *e;

    e = PQresultErrorField( get_pgresult( self), PG_DIAG_MESSAGE_DETAIL);
    return e == NULL ? Qnil : rb_str_new2( e);
}


/*
 * call-seq:
 *   pgqe.hint() => string
 *
 * Forward PostgreSQL's error hint.
 *
 */
VALUE
pgreserror_hint( self)
    VALUE self;
{
    char *e;

    e = PQresultErrorField( get_pgresult( self), PG_DIAG_MESSAGE_HINT);
    return e == NULL ? Qnil : rb_str_new2( e);
}


VALUE
pgresult_alloc( cls)
    VALUE cls;
{
    return Data_Wrap_Struct( cls, 0, &PQclear, NULL);
}


VALUE
pgresult_new( conn, result)
    PGconn   *conn;
    PGresult *result;
{
    VALUE res;

    res = pgresult_alloc( rb_cPgResult);
    DATA_PTR( res) = result;
    rb_obj_call_init( res, 0, NULL);
    return res;
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
pgresult_status( obj)
    VALUE obj;
{
    return INT2NUM( PQresultStatus( get_pgresult( obj)));
}


VALUE
fetch_fields( result)
    PGresult *result;
{
    VALUE ary;
    int n, i;
    VALUE str;

    n = PQnfields( result);
    ary = rb_ary_new2( n);
    for (i = 0; i < n; i++) {
        str = rb_tainted_str_new2( PQfname( result, i));
        rb_str_freeze( str);
        rb_ary_push( ary, str);
    }
    rb_ary_freeze( ary);
    return ary;
}

VALUE
field_index( fields, name)
    VALUE fields, name;
{
    VALUE ret;

    ret = rb_funcall( fields, id_index, 1, name);
    if (ret == Qnil)
        rb_raise( rb_ePgError, "%s: field not found", RSTRING_PTR( name));
    return ret;
}

VALUE
fetch_pgresult( result, row, column)
    PGresult *result;
    int row;
    int column;
{
    char *string;
    Oid typ;
    VALUE ret;

    if (PQgetisnull( result, row, column))
        return Qnil;

    string = PQgetvalue( result, row, column);

    if (!translate_results)
        return rb_tainted_str_new2( string);

    typ = PQftype( result, column);
    switch (typ) {
    case NUMERICOID:
        {
            int typmod;

            typmod = PQfmod( result, column);
            if (typmod == -1 || (typmod - VARHDRSZ) & 0xffff)
                break;
        }
        /* if scale == 0 fall through and return inum */
    case INT8OID:
    case INT4OID:
    case INT2OID:
    case OIDOID:
        return rb_cstr_to_inum( string, 10, 0);
    case FLOAT8OID:
    case FLOAT4OID:
        return rb_float_new( rb_cstr_to_dbl( string, Qfalse));
    case BOOLOID:
        return *string == 't' ? Qtrue : Qfalse;
    case BYTEAOID:
        ret = string_unescape_bytea( string);
        OBJ_TAINT( ret);
        return ret;
    default:
        break;
    }
    ret = rb_tainted_str_new2( string);
    switch (typ) {
    case NUMERICOID:
        return rb_funcall( rb_cBigDecimal, id_new, 1, ret);
    case DATEOID:
        return rb_funcall( rb_cDate, id_parse, 1, ret);
    case TIMEOID:
    case TIMETZOID:
        return rb_funcall( rb_cTime, id_parse, 1, ret);
    case TIMESTAMPOID:
    case TIMESTAMPTZOID:
        return rb_funcall( rb_cDateTime, id_parse, 1, ret);
    case CASHOID:
        return RTEST( pg_currency_class()) ?
                rb_funcall( rb_cCurrency, id_parse, 1, ret) : ret;
    default:
        return ret;
    }
}

VALUE
fetch_pgrow( result, row_num, fields)
    PGresult *result;
    int row_num;
    VALUE fields;
{
    VALUE row;
    int i, l;

    row = rb_funcall( rb_cPgRow, id_new, 1, fields);
    for (i = 0, l = RARRAY_LEN( row); l; ++i, --l)
        rb_ary_store( row, i, fetch_pgresult( result, row_num, i));
    return row;
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
pgresult_aref( argc, argv, obj)
    int argc;
    VALUE *argv;
    VALUE obj;
{
    PGresult *result;
    int nf, nt;
    VALUE a1, a2;
    int i, j;
    VALUE ret;

    result = get_pgresult( obj);
    nt = PQntuples( result);
    nf = PQnfields( result);
    ret = Qnil;
    switch (rb_scan_args( argc, argv, "11", &a1, &a2)) {
        case 1:
            i = NUM2INT( a1);
            if (i < nt)
                ret = fetch_pgrow( result, i, fetch_fields( result));
            break;
        case 2:
            i = NUM2INT( a1);
            if (i < nt) {
                if (TYPE( a2) == T_STRING)
                    a2 = field_index( fetch_fields( result), a2);
                j = NUM2INT( a2);
                if (j < nf)
                    ret = fetch_pgresult( result, i, j);
            }
            break;
        default:
            break;
    }
    return ret;
}

VALUE
string_unescape_bytea( char *escaped)
{
    unsigned char *s;
    size_t l;
    VALUE ret;

    s = PQunescapeBytea( (unsigned char *) escaped, &l);
    ret = rb_str_new( (char *) s, l);
    PQfreemem( s);
    return ret;
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
pgresult_each( self)
    VALUE self;
{
    PGresult *result;
    int n, r;
    VALUE fields;

    result = get_pgresult( self);
    fields = fetch_fields( result);
    for (n = 0, r = PQntuples( result); r; n++, r--)
        rb_yield( fetch_pgrow( result, n, fields));

    return n ? INT2NUM( n) : Qnil;
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
pgresult_fields( obj)
    VALUE obj;
{
    return fetch_fields( get_pgresult( obj));
}

/*
 * call-seq:
 *    res.num_tuples()
 *
 * Returns the number of tuples (rows) in the query result.
 *
 * Similar to <code>res.result.length</code> (but faster).
 */
VALUE
pgresult_num_tuples( obj)
    VALUE obj;
{
    return INT2NUM( PQntuples( get_pgresult( obj)));
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
pgresult_num_fields( obj)
    VALUE obj;
{
    return INT2NUM( PQnfields( get_pgresult( obj)));
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
pgresult_fieldname( obj, index)
    VALUE obj, index;
{
    PGresult *result;

    result = get_pgresult( obj);
    return rb_tainted_str_new2(
        PQfname( result, get_field_number( result, index)));
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
pgresult_fieldnum( obj, name)
    VALUE obj, name;
{
    int n;

    StringValue( name);
    n = PQfnumber( get_pgresult( obj), RSTRING_PTR( name));
    if (n == -1)
        rb_raise( rb_eArgError, "Unknown field: %s", RSTRING_PTR( name));
    return INT2NUM( n);
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
pgresult_type( obj, index)
    VALUE obj, index;
{
    PGresult* result = get_pgresult( obj);
    return INT2NUM(
        PQftype( result, get_field_number( result, index)));
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
pgresult_size( obj, index)
    VALUE obj, index;
{
    PGresult *result;

    result = get_pgresult( obj);
    return INT2NUM( PQfsize( result, get_field_number( result, index)));
}

/*
 * call-seq:
 *    res.value( tup_num, field_num)
 *
 * Returns the value in tuple number <i>tup_num</i>, field number
 * <i>field_num</i>. (Row <i>tup_num</i>, column <i>field_num</i>.)
 *
 * Equivalent to <code>res.result[<i>tup_num</i>][<i>field_num</i>]</code> (but
 * faster).
 */
VALUE
pgresult_getvalue( obj, tup_num, field_num)
    VALUE obj, tup_num, field_num;
{
    PGresult *result;

    result = get_pgresult( obj);
    return fetch_pgresult( result,
        get_tuple_number( result, tup_num),
        get_field_number( result, field_num));
}


/*
 * call-seq:
 *    res.value_byname( tup_num, field_name )
 *
 * Returns the value in tuple number <i>tup_num</i>, for the field named
 * <i>field_name</i>.
 *
 * Equivalent to (but faster than) either of:
 *    res.result[<i>tup_num</i>][ res.fieldnum(<i>field_name</i>) ]
 *    res.value( <i>tup_num</i>, res.fieldnum(<i>field_name</i>) )
 *
 * <i>(This method internally calls #value as like the second example above;
 * it is slower than using the field index directly.)</i>
 */
VALUE
pgresult_getvalue_byname( obj, tup_num, field_name)
    VALUE obj, tup_num, field_name;
{
    return pgresult_getvalue( obj, tup_num,
                pgresult_fieldnum( obj, field_name));
}

/*
 * call-seq:
 *    res.getlength( tup_num, field_num)  -> int
 *
 * Returns the (String) length of the field in bytes.
 *
 * Equivalent to <code>res.value(<i>tup_num</i>,<i>field_num</i>).length</code>.
 */
VALUE
pgresult_getlength( obj, tup_num, field_num)
    VALUE obj, tup_num, field_num;
{
    PGresult *result;

    result = get_pgresult( obj);
    return INT2FIX( PQgetlength( result,
        get_tuple_number( result, tup_num),
        get_field_number( result, field_num)));
}

/*
 * call-seq:
 *    res.getisnull( tuple_position, field_position) -> boolean
 *
 * Returns +true+ if the specified value is +nil+; +false+ otherwise.
 *
 * Equivalent to <code>res.value(<i>tup_num</i>,<i>field_num</i>)==+nil+</code>.
 */
VALUE
pgresult_getisnull( obj, tup_num, field_num)
    VALUE obj, tup_num, field_num;
{
    PGresult *result;

    result = get_pgresult( obj);
    return PQgetisnull( result,
        get_tuple_number( result, tup_num),
        get_field_number( result, field_num)) ? Qtrue : Qfalse;
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
pgresult_cmdtuples( obj)
    VALUE obj;
{
    char *n;

    n = PQcmdTuples( get_pgresult( obj));
    return *n ? rb_cstr_to_inum( n, 10, 0) : Qnil;
}

/*
 * call-seq:
 *    res.cmdstatus()
 *
 * Returns the status string of the last query command.
 */
VALUE
pgresult_cmdstatus( obj)
    VALUE obj;
{
    return rb_tainted_str_new2( PQcmdStatus( get_pgresult( obj)));
}

/*
 * call-seq:
 *    res.oid()  -> int
 *
 * Returns the +oid+.
 */
VALUE
pgresult_oid( obj)
    VALUE obj;
{
    Oid n;

    n = PQoidValue( get_pgresult( obj));
    return n == InvalidOid ? Qnil : INT2NUM( n);
}

/*
 * call-seq:
 *    res.clear()
 *
 * Clears the Pg::Result object as the result of the query.
 */
VALUE
pgresult_clear( obj)
    VALUE obj;
{
    if (DATA_PTR( obj) != NULL) {
      PQclear( get_pgresult( obj));
      DATA_PTR( obj) = NULL;
    }
    return Qnil;
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
#if 0
    rb_mPg = rb_define_module( "Pg");
#endif

    rb_require( "bigdecimal");
    rb_cBigDecimal = rb_const_get( rb_cObject, rb_intern( "BigDecimal"));

    rb_require( "date");
    rb_require( "time");
    rb_cDate       = rb_const_get( rb_cObject, rb_intern( "Date"));
    rb_cDateTime   = rb_const_get( rb_cObject, rb_intern( "DateTime"));
    rb_cCurrency   = Qnil;

    rb_cPgResult = rb_define_class_under( rb_mPg, "Result", rb_cObject);
    rb_define_alloc_func( rb_cPgResult, pgresult_alloc);
    rb_undef_method( CLASS_OF( rb_cPgResult), "new");
    rb_include_module( rb_cPgResult, rb_mEnumerable);

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

    rb_define_method( rb_cPgResult, "status", pgresult_status, 0);
    rb_define_alias( rb_cPgResult, "result", "entries");
    rb_define_alias( rb_cPgResult, "rows", "entries");
    rb_define_method( rb_cPgResult, "[]", pgresult_aref, -1);
    rb_define_method( rb_cPgResult, "each", pgresult_each, 0);
    rb_define_method( rb_cPgResult, "fields", pgresult_fields, 0);
    rb_define_method( rb_cPgResult, "num_tuples", pgresult_num_tuples, 0);
    rb_define_method( rb_cPgResult, "num_fields", pgresult_num_fields, 0);
    rb_define_method( rb_cPgResult, "fieldname", pgresult_fieldname, 1);
    rb_define_method( rb_cPgResult, "fieldnum", pgresult_fieldnum, 1);
    rb_define_method( rb_cPgResult, "type", pgresult_type, 1);
    rb_define_method( rb_cPgResult, "size", pgresult_size, 1);
    rb_define_method( rb_cPgResult, "getvalue", pgresult_getvalue, 2);
    rb_define_method( rb_cPgResult, "getvalue_byname",
                                                 pgresult_getvalue_byname, 2);
    rb_define_method( rb_cPgResult, "getlength", pgresult_getlength, 2);
    rb_define_method( rb_cPgResult, "getisnull", pgresult_getisnull, 2);
    rb_define_method( rb_cPgResult, "cmdtuples", pgresult_cmdtuples, 0);
    rb_define_method( rb_cPgResult, "cmdstatus", pgresult_cmdstatus, 0);
    rb_define_method( rb_cPgResult, "oid", pgresult_oid, 0);
    rb_define_method( rb_cPgResult, "clear", pgresult_clear, 0);
    rb_define_alias( rb_cPgResult, "close", "clear");


    rb_ePgResError = rb_define_class_under( rb_cPgResult, "Error", rb_ePgError);
    rb_define_alloc_func( rb_ePgResError, pgresult_alloc);

    rb_define_attr( rb_ePgResError, "command",    1, 0);
    rb_define_attr( rb_ePgResError, "parameters", 1, 0);

    rb_define_method( rb_ePgResError, "status", pgreserror_status, 0);
    rb_define_method( rb_ePgResError, "sqlstate", pgreserror_sqlst, 0);
    rb_define_alias( rb_ePgResError, "errcode", "sqlstate");
    rb_define_method( rb_ePgResError, "primary", pgreserror_primary, 0);
    rb_define_method( rb_ePgResError, "details", pgreserror_detail, 0);
    rb_define_method( rb_ePgResError, "hint", pgreserror_hint, 0);


    id_parse    = rb_intern( "parse");
    id_index    = rb_intern( "index");
    id_currency = rb_intern( "Currency");
}

