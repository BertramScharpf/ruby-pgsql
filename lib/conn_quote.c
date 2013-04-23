/*
 *  conn_quote.c  --  PostgreSQL connection, string handling
 */


#include "conn_quote.h"


extern VALUE pg_currency_class( void);

static VALUE pgconn_format( VALUE self, VALUE obj);

static VALUE pgconn_escape_bytea(   VALUE self, VALUE obj);
static VALUE pgconn_unescape_bytea( VALUE self, VALUE obj);
extern VALUE string_unescape_bytea( char *escaped);

extern VALUE pgconn_stringize( VALUE self, VALUE obj);
extern VALUE pgconn_stringize_line( VALUE self, VALUE ary);
static int   needs_dquote_string( VALUE str);
static VALUE dquote_string( VALUE str);
static VALUE stringize_array( VALUE self, VALUE result, VALUE ary);
static VALUE gsub_escape_i( VALUE c, VALUE arg);

static VALUE pgconn_quote_bytea( VALUE self, VALUE obj);
static VALUE pgconn_quote( VALUE self, VALUE value);
static VALUE pgconn_quote_all( int argc, VALUE *argv, VALUE self);
static VALUE quote_string( VALUE conn, VALUE str);
static VALUE quote_array( VALUE self, VALUE result, VALUE ary);
static void  quote_all( VALUE self, VALUE ary, VALUE res);

static VALUE pgconn_quote_identifier( VALUE self, VALUE value);



VALUE rb_cDate;
VALUE rb_cDateTime;
VALUE rb_cCurrency;

static ID id_format;
static ID id_iso8601;
static ID id_raw;
static ID id_to_postgres;
static ID id_gsub_bang;
static ID id_currency;

static const char *str_NULL = "NULL";
static VALUE pg_escape_regex;



VALUE
pg_currency_class( void)
{
    if (NIL_P( rb_cCurrency) && rb_const_defined( rb_cObject, id_currency))
        rb_cCurrency = rb_const_get( rb_cObject, id_currency);
    return rb_cCurrency;
}



/*
 * call-seq:
 *   conn.format( obj)  -> obj
 *
 * Format an object before it will be made a PostgreSQL type.
 * By default this just returns the unmodified object but you may
 * overwrite it fitting you own needs.
 *
 * The Object won't be replaced if this method returns +nil+.
 *
 * = Example
 *
 *   class MyConn < Pg::Conn
 *     def format obj
 *       case obj
 *         when Currency then obj.to_s_by_locale
 *         else               obj
 *       end
 *     end
 *   end
 */
VALUE
pgconn_format( VALUE self, VALUE obj)
{
    return obj;
}


/*
 * call-seq:
 *   conn.escape_bytea( str)  -> str
 *
 * Converts a string of binary data into an escaped version.
 * Example:
 *
 *   conn.escape_bytea "abc"    # => "\\x616263"
 *                              # (One backslash, then an 'x'.)
 *
 * This is what you need, when you pass your object as a Conn#exec parameter,
 * as a +COPY+ input line or as a subject to +Conn#quote+-ing.
 *
 * If you execute an +INSERT+ statement and mention you object in the statement
 * string you should call Conn.quote_bytea().
 *
 * See the PostgreSQL documentation on PQescapeByteaConn
 * [http://www.postgresql.org/docs/current/interactive/libpq-exec.html#LIBPQ-PQESCAPEBYTEACONN]
 * for more information.
 */
VALUE
pgconn_escape_bytea( VALUE self, VALUE str)
{
    unsigned char *s;
    size_t l;
    char nib[ 3];
    VALUE ret;

    StringValue( str);
    ret = rb_str_buf_new2( "\\x");
    for (s = (unsigned char *) RSTRING_PTR( str), l = RSTRING_LEN( str); l;
                            ++s, --l) {
        sprintf( nib, "%02x", (int) *s);
        rb_str_buf_cat( ret, nib, 2);
    }
    OBJ_INFECT( ret, str);
    return ret;
}

/*
 * call-seq:
 *   conn.unescape_bytea( str)  -> str
 *
 * Converts an escaped string representation of binary data into binary data.
 * Example:
 *
 *   Pg::Conn.unescape_bytea "\\x616263"   # =>  "abc"
 *
 * Normally you will not need this because Pg::Result's methods will convert a
 * return value automatically if the field type was a +bytea+.
 *
 * See the PostgreSQL documentation on PQunescapeBytea
 * [http://www.postgresql.org/docs/current/interactive/libpq-exec.html#LIBPQ-PQUNESCAPEBYTEA]
 * for more information.
 */
VALUE
pgconn_unescape_bytea( VALUE self, VALUE str)
{
    VALUE ret;

    StringValue( str);
    ret = string_unescape_bytea( RSTRING_PTR( str));
    OBJ_INFECT( ret, str);
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
 *   conn.stringize( obj) -> str
 *
 * This methods makes a string out of everything.  Numbers, booleans, +nil+,
 * date and time values, and even arrays will be written a string as PostgreSQL
 * accepts constants.  You may pass the result as a field after a +COPY+
 * statement. This will be called internally for the parameters to +exec+,
 * +query+ etc.
 *
 * Any other objects will be checked whether they have a method named
 * +to_postgres+.  If that doesn't exist the object will be converted by
 * +to_s+.
 *
 * If you are quoting into a SQL statement please don't do something like
 * <code>"insert into ... (E'#{conn.stringize obj}', ...);"</code>.  Use
 * +Conn.quote+ instead that will put the appropriate quoting characters around
 * its string.
 *
 * If you like to pass a +bytea+ you have to escape the string yourself.
 * This library cannot decide itself whether a String object is meant as a
 * string or as a +bytea+.  See the Pg::Conn#escape_bytea method.
 */
VALUE
pgconn_stringize( VALUE self, VALUE obj)
{
    VALUE o, result;

    o = rb_funcall( self, id_format, 1, obj);
    if (!NIL_P( o))
        obj = o;
    switch (TYPE( obj)) {
        case T_STRING:
            result = obj;
            break;

        case T_NIL:
            result = rb_str_new2( str_NULL);
            break;

        case T_TRUE:
        case T_FALSE:
            result = rb_obj_as_string( obj);
            break;

        case T_BIGNUM:
        case T_FLOAT:
        case T_FIXNUM:
            result = rb_obj_as_string( obj);
            break;

        case T_ARRAY:
            result = rb_str_buf_new2( "{");
            stringize_array( self, result, obj);
            rb_str_buf_cat2( result, "}");
            break;

        default:
            if (rb_obj_is_kind_of( obj, rb_cNumeric))
                result = rb_obj_as_string( obj);
            else {
                VALUE co;

                co = CLASS_OF( obj);
                if        (co == rb_cTime) {
                    result = rb_funcall( obj, id_iso8601, 0);
                    OBJ_INFECT( result, obj);
                } else if (co == rb_cDate)
                    result = rb_obj_as_string( obj);
                else if   (co == rb_cDateTime)
                    result = rb_obj_as_string( obj);
                else if   (co == pg_currency_class() &&
                                    rb_respond_to( obj, id_raw))
                    result = rb_funcall( obj, id_raw, 0);
                else if   (rb_respond_to( obj, id_to_postgres)) {
                    result = rb_funcall( obj, id_to_postgres, 0);
                    StringValue( result);
                    OBJ_INFECT( result, obj);
                } else
                    result = rb_obj_as_string( obj);
            }
            break;
    }
    return result;
}

/*
 * call-seq:
 *    conn.stringize_line( ary)  ->  str
 *
 * Quote a line the standard way that +COPY+ expects. Tabs, newlines, and
 * backslashes will be escaped, +nil+ will become "\\N".
 */
VALUE
pgconn_stringize_line( VALUE self, VALUE ary)
{
    VALUE a;
    VALUE *p;
    int l;
    VALUE ret, s;

    a = rb_check_convert_type( ary, T_ARRAY, "Array", "to_ary");
    if (NIL_P(a))
        rb_raise( rb_eArgError, "Give me an array.");
    if (NIL_P( pg_escape_regex)) {
        pg_escape_regex = rb_reg_new( "([\\b\\f\\n\\r\\t\\v\\\\])", 18, 0);
        rb_global_variable( &pg_escape_regex);
    }
    ret = rb_str_new( NULL, 0);
    for (l = RARRAY_LEN( a), p = RARRAY_PTR( a); l; ++p) {
        if (NIL_P( *p))
            rb_str_cat( ret, "\\N", 2);
        else {
            s = pgconn_stringize( self, *p);
            rb_block_call( s, id_gsub_bang, 1, &pg_escape_regex,
                                                    gsub_escape_i, Qnil);
            rb_str_cat( ret, RSTRING_PTR( s), RSTRING_LEN( s));
        }
        if (--l > 0)
            rb_str_cat( ret, "\t", 1);
        else
            rb_str_cat( ret, "\n", 1);
    }
    return ret;
}



int
needs_dquote_string( VALUE str)
{
    char *p;
    long l;

    if (strcmp( RSTRING_PTR( str), str_NULL) == 0)
        return 1;
    l = RSTRING_LEN( str);
    if (l == 0)
        return 1;
    for (p = RSTRING_PTR( str); l; ++p, --l)
        if (*p == ',' || *p == ' ' || *p == '\\' || *p == '"')
            break;
    return l > 0;
}

VALUE
dquote_string( VALUE str)
{
    VALUE ret;

    ret = str;
    if (needs_dquote_string( str)) {
        char *p, *q;
        long l, m;

        ret = rb_str_buf_new2( "\"");
        p = RSTRING_PTR( str);
        l = RSTRING_LEN( str);
        while (l) {
            q = p, m = l;
            for (; m && (*q != '"' && *q != '\\'); --m, ++q)
                ;
            rb_str_buf_cat( ret, p, l - m);
            if (m) {
                rb_str_buf_cat2( ret, "\\");
                rb_str_buf_cat( ret, q, 1);
                --m, ++q;
            }
            p = q, l = m;
        }
        rb_str_buf_cat2( ret, "\"");
    }
    return ret;
}

VALUE
stringize_array( VALUE self, VALUE result, VALUE ary)
{
    long i, j;
    VALUE *o;
    VALUE cf, co;
    VALUE r;

    cf = Qundef;
    for (o = RARRAY_PTR( ary), j = RARRAY_LEN( ary); j; ++o, --j) {
        co = CLASS_OF( *o);
        if (cf == Qundef)
            cf = co;
        else {
            if (co != cf)
                rb_raise( rb_ePgError, "Array members of different type.");
            rb_str_buf_cat2( result, ",");
        }
        r = pgconn_stringize( self, *o);
        if (!NIL_P( *o)) {
            r = dquote_string( r);
            OBJ_INFECT( result, *o);
        }
        rb_str_buf_append( result, r);
    }
    return result;
}


VALUE
gsub_escape_i( VALUE c, VALUE arg)
{
    const char *r;

    r = NULL;
    switch (*RSTRING_PTR( c)) {
        case '\b': r = "\\b";  break;
        case '\f': r = "\\f";  break;
        case '\n': r = "\\n";  break;
        case '\r': r = "\\r";  break;
        case '\t': r = "\\t";  break;
        case '\v': r = "\\v";  break;
        case '\\': r = "\\\\"; break;
        default:               break;
    }
    return rb_str_new2( r);
}




/*
 * call-seq:
 *   conn.quote_bytea( str)  -> str
 *
 * Converts a string of binary data into an escaped version.
 * Example:
 *
 *   conn.quote_bytea "abc"    # => "E'\\\\x616263'::bytea"
 *                             # (Two backslashes, then an 'x'.)
 *
 * This is what you need, when you execute an +INSERT+ statement and mention
 * you object in the statement string.
 *
 * If you pass your object as a Conn#exec parameter, as a +COPY+ input line or
 * as a subject to +Conn#quote+-ing you should call Conn.escape_bytea().
 *
 * See the PostgreSQL documentation on PQescapeByteaConn
 * [http://www.postgresql.org/docs/current/interactive/libpq-exec.html#LIBPQ-PQESCAPEBYTEACONN]
 * for more information.
 */
VALUE
pgconn_quote_bytea( VALUE self, VALUE str)
{
    char *t;
    size_t l;
    VALUE ret;

    StringValue( str);
    t = (char *) PQescapeByteaConn( get_pgconn( self)->conn,
            (unsigned char *) RSTRING_PTR( str), RSTRING_LEN( str), &l);
    ret = rb_str_buf_new2( " E'");
    rb_str_buf_cat( ret, t, l - 1);
    rb_str_buf_cat2( ret, "'::bytea");
    PQfreemem( t);
    OBJ_INFECT( ret, str);
    return ret;
}

/*
 * call-seq:
 *   conn.quote( obj) -> str
 *
 * This methods makes a PostgreSQL constant out of everything.  You may mention
 * any result in a statement passed to Conn#exec.
 *
 * If you prefer to pass your objects as a parameter to +exec+, +query+ etc.
 * or as a field after a +COPY+ statement you should call conn#stringize.
 *
 * This method is to prevent you from saying something like
 * <code>"insert into ... (E'#{conn.stringize obj}', ...);"</code>.  It is
 * more efficient to say
 *
 *   conn.exec "insert into ... (#{conn.quote obj}, ...);"
 *
 * Your self-defined classes will be checked whether they have a method named
 * +to_postgres+.  If that doesn't exist the object will be converted by
 * +to_s+.
 *
 * Call Pg::Conn#quote_bytea if you want to tell your string is a byte array.
 */
VALUE
pgconn_quote( VALUE self, VALUE obj)
{
    VALUE o, result;

    o = rb_funcall( self, id_format, 1, obj);
    if (!NIL_P( o))
        obj = o;
    switch (TYPE( obj)) {
        case T_STRING:
            return quote_string( self, obj);
        case T_NIL:
            return rb_str_new2( str_NULL);
        case T_TRUE:
        case T_FALSE:
        case T_FIXNUM:
            return rb_obj_as_string( obj);
        case T_BIGNUM:
        case T_FLOAT:
            return rb_obj_as_string( obj);

        case T_ARRAY:
            result = rb_str_buf_new2( "ARRAY[");
            quote_array( self, result, obj);
            rb_str_buf_cat2( result, "]");
            break;

        default:
            if (rb_obj_is_kind_of( obj, rb_cNumeric))
                result = rb_obj_as_string( obj);
            else {
                VALUE co;
                char *type;

                co = CLASS_OF( obj);
                if        (co == rb_cTime) {
                    result = rb_funcall( obj, id_iso8601, 0);
                    type = "timestamptz";
                } else if (co == rb_cDate) {
                    result = rb_obj_as_string( obj);
                    type = "date";
                } else if (co == rb_cDateTime) {
                    result = rb_obj_as_string( obj);
                    type = "timestamptz";
                } else if (co == pg_currency_class() &&
                                    rb_respond_to( obj, id_raw)) {
                    result = rb_funcall( obj, id_raw, 0);
                    StringValue( result);
                    type = "money";
                } else if (rb_respond_to( obj, id_to_postgres)) {
                    result = rb_funcall( obj, id_to_postgres, 0);
                    StringValue( result);
                    type = NULL;
                } else {
                    result = rb_obj_as_string( obj);
                    type = "unknown";
                }
                result = quote_string( self, result);
                if (type != NULL) {
                    rb_str_buf_cat2( result, "::");
                    rb_str_buf_cat2( result, type);
                }
                OBJ_INFECT( result, obj);
            }
            break;
    }
    return result;
}

/*
 * call-seq:
 *   conn.quote_all( *args) -> str
 *
 * Does a #quote for every argument and pastes the results
 * together with comma.
 */
VALUE
pgconn_quote_all( int argc, VALUE *argv, VALUE self)
{
    VALUE res;
    VALUE args;

    res = rb_str_new( NULL, 0);
    rb_scan_args( argc, argv, "0*", &args);
    quote_all( self, args, res);
    return res;
}

VALUE
quote_string( VALUE conn, VALUE str)
{
    char *p;
    VALUE result;

    p = PQescapeLiteral( get_pgconn( conn)->conn, RSTRING_PTR( str), RSTRING_LEN( str));
    result = rb_str_new2( p);
    PQfreemem( p);
    OBJ_INFECT( result, str);
    return result;
}

VALUE
quote_array( VALUE self, VALUE result, VALUE ary)
{
    long i, j;
    VALUE *o;
    VALUE cf, co;

    cf = Qundef;
    for (o = RARRAY_PTR( ary), j = RARRAY_LEN( ary); j; ++o, --j) {
        co = CLASS_OF( *o);
        if (cf == Qundef)
            cf = co;
        else {
            if (co != cf)
                rb_raise( rb_ePgError, "Array members of different type.");
            rb_str_buf_cat2( result, ",");
        }
        rb_str_buf_append( result, pgconn_quote( self, *o));
    }
    return result;
}

void
quote_all( VALUE self, VALUE ary, VALUE res)
{
    VALUE *p;
    long len;

    for (p = RARRAY_PTR( ary), len = RARRAY_LEN( ary); len; len--, p++) {
        if (TYPE( *p) == T_ARRAY)
            quote_all( self, *p, res);
        else {
            if (RSTRING_LEN( res) > 0)
                rb_str_buf_cat2( res, ",");
            rb_str_buf_append( res, pgconn_quote( self, *p));
        }
    }
}



/*
 * call-seq:
 *    conn.quote_identifier() -> str
 *
 * Put double quotes around an identifier containing non-letters
 * or upper case.
 */
VALUE
pgconn_quote_identifier( VALUE self, VALUE str)
{
    char *p;
    VALUE result;

    StringValue( str);
    p = PQescapeIdentifier( get_pgconn( self)->conn, RSTRING_PTR( str), RSTRING_LEN( str));
    result = rb_str_new2( p);
    PQfreemem( p);
    OBJ_INFECT( result, str);
    return result;
}



void
Init_pgsql_conn_quote( void)
{
    rb_require( "date");
    rb_require( "time");
    rb_cDate       = rb_const_get( rb_cObject, rb_intern( "Date"));
    rb_cDateTime   = rb_const_get( rb_cObject, rb_intern( "DateTime"));
    rb_cCurrency   = Qnil;

    rb_define_method( rb_cPgConn, "format", pgconn_format, 1);

    rb_define_method( rb_cPgConn, "escape_bytea", pgconn_escape_bytea, 1);
    rb_define_singleton_method( rb_cPgConn, "escape_bytea", pgconn_escape_bytea, 1);
    rb_define_method( rb_cPgConn, "unescape_bytea", pgconn_unescape_bytea, 1);
    rb_define_singleton_method( rb_cPgConn, "unescape_bytea", pgconn_unescape_bytea, 1);

    rb_define_method( rb_cPgConn, "stringize", pgconn_stringize, 1);
    rb_define_method( rb_cPgConn, "stringize_line", pgconn_stringize_line, 1);

    rb_define_method( rb_cPgConn, "quote_bytea", pgconn_quote_bytea, 1);
    rb_define_method( rb_cPgConn, "quote", pgconn_quote, 1);
    rb_define_method( rb_cPgConn, "quote_all", pgconn_quote_all, -1);
    rb_define_alias( rb_cPgConn, "q", "quote_all");

    rb_define_method( rb_cPgConn, "quote_identifier", pgconn_quote_identifier, 1);
    rb_define_alias( rb_cPgConn, "quote_ident", "quote_identifier");

    id_format      = rb_intern( "format");
    id_iso8601     = rb_intern( "iso8601");
    id_raw         = rb_intern( "raw");
    id_to_postgres = rb_intern( "to_postgres");
    id_gsub_bang   = rb_intern( "gsub!");

    id_currency    = rb_intern( "Currency");

    pg_escape_regex = Qnil;
}

