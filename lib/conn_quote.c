/*
 *  conn_quote.c  --  PostgreSQL connection, string handling
 */


#include "conn_quote.h"


extern VALUE pg_monetary_class( void);

static VALUE pgconn_format( VALUE self, VALUE obj);

static VALUE pgconn_escape_bytea(   VALUE self, VALUE str);
#ifdef RUBY_ENCODING
static VALUE pgconn_unescape_bytea( int argc, VALUE *argv, VALUE self);
#else
static VALUE pgconn_unescape_bytea( VALUE self, VALUE obj);
#endif

extern VALUE pgconn_stringize( VALUE self, VALUE obj);
extern VALUE pgconn_stringize_line( VALUE self, VALUE ary);
extern VALUE pgconn_for_copy( VALUE self, VALUE str);
static int   needs_dquote_string( VALUE str);
static VALUE dquote_string( VALUE str);
static VALUE stringize_array( VALUE self, VALUE result, VALUE ary);
static VALUE gsub_escape_i( RB_BLOCK_CALL_FUNC_ARGLIST( c, arg));


static VALUE pgconn_quote( VALUE self, VALUE obj);
static VALUE pgconn_quote_all( int argc, VALUE *argv, VALUE self);
static VALUE quote_string( VALUE conn, VALUE str);
static VALUE quote_array( VALUE self, VALUE result, VALUE ary);
static void  quote_all( VALUE self, VALUE ary, VALUE res);

static VALUE pgconn_quote_identifier( VALUE self, VALUE value);


VALUE rb_cDate;
VALUE rb_cDateTime;

static VALUE rb_cMoney;

static ID id_format;
static ID id_iso8601;
static ID id_raw;
static ID id_to_postgres;
static ID id_gsub;

static int lookup_monetary;

static VALUE pg_string_null;
static VALUE pg_string_bsl_N;
static VALUE pg_escape_regex;


static const char *monetary[] = { "Money", "Monetary", "Amount", "Currency"};

/*
 * Document-class: Pg::Money
 *
 * The class the PostgreSQL-builtin type +MONEY+ wil be mapped to.
 * If not set, some global classes will be searched
 * (+Money+, +Monetary+, +Amount+, +Currency+) and the constant will be
 * set to that.
 *
 * Set this constant to your own class before you do the first query,
 * or set it to +nil+ to inhibit the auto-detect.
 *
 * The resulting class must have a method +parse+ to build a new object
 * from a string. It may respond to +raw+ which overwrites +to_s+.
 *
 */
VALUE
pg_monetary_class( void)
{
    if (lookup_monetary) {
        ID id_MONEY = rb_intern( "Money");
        if (rb_const_defined( rb_mPg, id_MONEY))
            rb_cMoney = rb_const_get( rb_mPg, id_MONEY);
        else {
            int i;
            for (i = 0; i < (sizeof monetary) / sizeof (char *) && NIL_P( rb_cMoney); i++) {
                ID id = rb_intern( monetary[ i]);
                if (rb_const_defined( rb_cObject, id))
                    rb_cMoney = rb_const_get( rb_cObject, id);
            }
            rb_const_set( rb_mPg, id_MONEY, rb_cMoney);
        }
        lookup_monetary = 0;
    }
    return rb_cMoney;
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
 * == Example
 *
 *   class MyConn < Pg::Conn
 *     def format obj
 *       case obj
 *         when Money then obj.to_s_by_locale
 *         else            obj
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
 *
 * == Example
 *
 *   conn.escape_bytea "abc"    # => "\\x616263"
 *                              # (One backslash, then an 'x'.)
 *
 * This is what you need, when you pass your object as a Conn#exec parameter,
 * as a +COPY+ input line or as a subject to +Conn#quote+-ing.
 *
 * If you execute an +INSERT+ statement and mention your object in the statement
 * string via "#{}" you should call Conn.quote() after this.
 *
 * Note that the encoding does not have any influence.  The bytes will be written
 * as if <code>String#each_byte</code> would have been called.
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
    VALUE ret;

    if (NIL_P( str))
        return Qnil;
    StringValue( str);
    s = PQescapeByteaConn( get_pgconn( self)->conn,
            (unsigned char *) RSTRING_PTR( str), RSTRING_LEN( str), &l);
    ret = rb_str_new( (char *) s, l - 1);
    PQfreemem( s);
    OBJ_INFECT( ret, str);
    return ret;
}

/*
 * call-seq:
 *   conn.unescape_bytea( str, enc = nil)  -> str
 *   conn.unescape_bytea( str)             -> str    (Ruby 1.8)
 *
 * Converts an escaped string into binary data.
 *
 * == Example
 *
 *   Pg::Conn.unescape_bytea "\\x616263"   # =>  "abc"
 *
 * You will need this because Pg::Result will not convert a return value
 * automatically if the field type was a +bytea+.
 *
 * If +enc+ is given, the result will be associated with this encoding.
 * A conversion will not be tried.  Probably, if dealing with encodings
 * you should store the encoding in another column next to the data.
 *
 * See the PostgreSQL documentation on PQunescapeBytea
 * [http://www.postgresql.org/docs/current/interactive/libpq-exec.html#LIBPQ-PQUNESCAPEBYTEA]
 * for more information.
 */
#ifdef RUBY_ENCODING
VALUE
pgconn_unescape_bytea( int argc, VALUE *argv, VALUE self)
#else
VALUE
pgconn_unescape_bytea( VALUE self, VALUE obj)
#endif
{
#ifdef RUBY_ENCODING
    VALUE obj, enc;
#endif
    unsigned char *s;
    size_t l;
    VALUE ret;

#ifdef RUBY_ENCODING
    rb_scan_args( argc, argv, "11", &obj, &enc);
#endif
    if (NIL_P( obj))
        return Qnil;
    StringValue( obj);

    s = PQunescapeBytea( (unsigned char *) RSTRING_PTR( obj), &l);
    ret = rb_str_new( (char *) s, l);
    PQfreemem( s);

#ifdef RUBY_ENCODING
    if (!NIL_P( enc))
        rb_enc_associate( ret, rb_to_encoding( enc));
#endif

    OBJ_INFECT( ret, obj);
    return ret;
}


/*
 * call-seq:
 *   conn.stringize( obj) -> str
 *
 * This methods makes a string out of everything.  Numbers, booleans, +nil+,
 * date and time values, and even arrays will be written as string the way
 * PostgreSQL accepts constants.  You may pass the result as a field after a
 * +COPY+ statement.  This will be called internally for the parameters to
 * +exec+, +query+ etc.
 *
 * Any other objects will be checked whether they have a method named
 * +to_postgres+.  If that doesn't exist the object will be converted by
 * +to_s+.
 *
 * If you are quoting into a SQL statement please don't do something like
 * <code>"insert into ... (E'#{conn.stringize obj}', ...);"</code>.  Use
 * +Conn.quote+ instead that will put the appropriate quoting characters around
 * its strings.
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
            result = pg_string_null;
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
                else if   (co == pg_monetary_class() &&
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
 * Quote a line the standard way that +COPY+ expects.  Tabs, newlines, and
 * backslashes will be escaped, +nil+ will become "\\N".
 */
VALUE
pgconn_stringize_line( VALUE self, VALUE ary)
{
    VALUE a;
    VALUE *p;
    int l;
    VALUE ret;

    a = rb_check_convert_type( ary, T_ARRAY, "Array", "to_ary");
    if (NIL_P(a))
        rb_raise( rb_eArgError, "Give me an array.");
    ret = rb_str_new( NULL, 0);
    for (l = RARRAY_LEN( a), p = RARRAY_PTR( a); l; ++p) {
        rb_str_concat( ret, pgconn_for_copy( self, *p));
        rb_str_cat( ret, (--l > 0 ? "\t" : "\n"), 1);
    }
    return ret;
}

/*
 * call-seq:
 *    conn.for_copy( obj)  ->  str
 *
 * Quote for +COPY+ expects. +nil+ will become "\\N".
 *
 * Then, tabs, newlines, and backslashes will be escaped.
 */
VALUE
pgconn_for_copy( VALUE self, VALUE obj)
{
    VALUE ret;

    if (NIL_P( obj))
        ret = pg_string_bsl_N;
    else {
        ret = pgconn_stringize( self, obj);
        if (NIL_P( pg_escape_regex)) {
            pg_escape_regex = rb_reg_new( "([\\b\\f\\n\\r\\t\\v\\\\])", 18, 0);
            rb_global_variable( &pg_escape_regex);
        }
        if (RTEST( rb_reg_match( pg_escape_regex, ret)))
            ret = rb_block_call( ret, id_gsub, 1, &pg_escape_regex, &gsub_escape_i, Qnil);
    }
    return ret;
}


int
needs_dquote_string( VALUE str)
{
    char *p;
    long l;

    if (rb_str_cmp( str, pg_string_null) == 0)
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
        rb_enc_associate( ret, rb_enc_get( str));
    }
    return ret;
}

VALUE
stringize_array( VALUE self, VALUE result, VALUE ary)
{
    long i;
    VALUE *o;
    VALUE cf, co;
    VALUE r;

    cf = Qundef;
    for (o = RARRAY_PTR( ary), i = RARRAY_LEN( ary); i; ++o, --i) {
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
gsub_escape_i( RB_BLOCK_CALL_FUNC_ARGLIST( c, arg))
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
 * call-seq: conn.quote( obj) -> str
 *
 * This methods makes a PostgreSQL constant out of everything.  You may mention
 * any result in a statement passed to Conn#exec via "#{}".
 *
 * If you prefer to pass your objects as a parameter to +exec+, +query+ etc. or
 * as a field after a +COPY+ statement you should call conn#stringize.
 *
 * This method is to prevent you from saying something like <code>"insert into
 * ... (E'#{conn.stringize obj}', ...);"</code>.  It is more efficient to say
 *
 *   conn.exec "INSERT INTO ... (#{conn.quote obj}, ...);"
 *
 * Your self-defined classes will be checked whether they have a method named
 * +to_postgres+.  If that doesn't exist the object will be converted by
 * +to_s+.
 *
 * Call Pg::Conn#escape_bytea first if you want to tell your string is a byte
 * array and the quote that result.
 */
VALUE pgconn_quote( VALUE self, VALUE obj)
{
    VALUE o, res;

    o = rb_funcall( self, id_format, 1, obj);
    if (!NIL_P( o))
        obj = o;
    switch (TYPE( obj)) {
        case T_STRING:
            return quote_string( self, obj);
        case T_NIL:
            return pg_string_null;
        case T_TRUE:
        case T_FALSE:
        case T_FIXNUM:
            return rb_obj_as_string( obj);
        case T_BIGNUM:
        case T_FLOAT:
            return rb_obj_as_string( obj);

        case T_ARRAY:
            res = rb_str_buf_new2( "ARRAY[");
            quote_array( self, res, obj);
            rb_str_buf_cat2( res, "]");
            break;

        default:
            if (rb_obj_is_kind_of( obj, rb_cNumeric))
                res = rb_obj_as_string( obj);
            else {
                VALUE co;
                char *type;

                co = CLASS_OF( obj);
                if        (co == rb_cTime) {
                    res = rb_funcall( obj, id_iso8601, 0);
                    type = "TIMESTAMPTZ";
                } else if (co == rb_cDate) {
                    res = rb_obj_as_string( obj);
                    type = "DATE";
                } else if (co == rb_cDateTime) {
                    res = rb_obj_as_string( obj);
                    type = "TIMESTAMPTZ";
                } else if (co == pg_monetary_class() &&
                                    rb_respond_to( obj, id_raw)) {
                    res = rb_funcall( obj, id_raw, 0);
                    StringValue( res);
                    type = "MONEY";
                } else if (rb_respond_to( obj, id_to_postgres)) {
                    res = rb_funcall( obj, id_to_postgres, 0);
                    StringValue( res);
                    type = NULL;
                } else {
                    res = rb_obj_as_string( obj);
                    type = "UNKNOWN";
                }
                res = quote_string( self, res);
                if (type != NULL) {
                    rb_str_buf_cat2( res, "::");
                    rb_str_buf_cat2( res, type);
                }
                OBJ_INFECT( res, obj);
            }
            break;
    }
    return res;
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
    VALUE res;

    p = PQescapeLiteral( get_pgconn( conn)->conn, RSTRING_PTR( str), RSTRING_LEN( str));
    res = rb_str_new2( p);
    PQfreemem( p);
    rb_enc_associate( res, rb_enc_get( str));
    OBJ_INFECT( res, str);
    return res;
}

VALUE
quote_array( VALUE self, VALUE result, VALUE ary)
{
    long i;
    VALUE *o;
    VALUE cf, co;

    cf = Qundef;
    for (o = RARRAY_PTR( ary), i = RARRAY_LEN( ary); i; ++o, --i) {
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
    VALUE res;

    if (NIL_P( str))
        return Qnil;
    StringValue( str);
    p = PQescapeIdentifier( get_pgconn( self)->conn, RSTRING_PTR( str), RSTRING_LEN( str));
    res = rb_str_new2( p);
    PQfreemem( p);
    rb_enc_associate( res, rb_enc_get( str));
    OBJ_INFECT( res, str);
    return res;
}



void
Init_pgsql_conn_quote( void)
{
    rb_cDate       = rb_const_get( rb_cObject, rb_intern( "Date"));
    rb_cDateTime   = rb_const_get( rb_cObject, rb_intern( "DateTime"));
    rb_cMoney      = Qnil;

#ifdef RDOC_NEEDS_THIS
    rb_cPgConn = rb_define_class_under( rb_mPg, "Conn", rb_cObject);
#endif

    rb_define_method( rb_cPgConn, "format", &pgconn_format, 1);

    rb_define_method( rb_cPgConn, "escape_bytea", &pgconn_escape_bytea, 1);
#ifdef RUBY_ENCODING
    rb_define_method( rb_cPgConn, "unescape_bytea", &pgconn_unescape_bytea, -1);
    rb_define_singleton_method( rb_cPgConn, "unescape_bytea", &pgconn_unescape_bytea, -1);
#else
    rb_define_method( rb_cPgConn, "unescape_bytea", &pgconn_unescape_bytea, 1);
    rb_define_singleton_method( rb_cPgConn, "unescape_bytea", &pgconn_unescape_bytea, 1);
#endif

    rb_define_method( rb_cPgConn, "stringize", &pgconn_stringize, 1);
    rb_define_method( rb_cPgConn, "stringize_line", &pgconn_stringize_line, 1);
    rb_define_method( rb_cPgConn, "for_copy", &pgconn_for_copy, 1);

    rb_define_method( rb_cPgConn, "quote", &pgconn_quote, 1);
    rb_define_method( rb_cPgConn, "quote_all", &pgconn_quote_all, -1);
    rb_define_alias( rb_cPgConn, "q", "quote_all");

    rb_define_method( rb_cPgConn, "quote_identifier", &pgconn_quote_identifier, 1);
    rb_define_alias( rb_cPgConn, "quote_ident", "quote_identifier");

    id_format      = rb_intern( "format");
    id_iso8601     = rb_intern( "iso8601");
    id_raw         = rb_intern( "raw");
    id_to_postgres = rb_intern( "to_postgres");
    id_gsub        = rb_intern( "gsub");

    lookup_monetary = 1;

    pg_string_null  = rb_str_new2( "NULL");  rb_global_variable( &pg_string_null);   rb_str_freeze( pg_string_null);
    pg_string_bsl_N = rb_str_new2( "\\N");   rb_global_variable( &pg_string_bsl_N);  rb_str_freeze( pg_string_bsl_N);
    pg_escape_regex = Qnil;
}

