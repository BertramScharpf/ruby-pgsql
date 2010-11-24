/*
 *  row.c  --  Pg query rows
 */


#include "row.h"


static ID id_keys;

static VALUE pgrow_init( VALUE self, VALUE keys);
static VALUE pgrow_aref( int argc, VALUE * argv, VALUE self);
static VALUE pgrow_keys( VALUE self);
static VALUE pgrow_values( VALUE self);
static VALUE pgrow_each( VALUE self);
static VALUE pgrow_each_pair( VALUE self);
static VALUE pgrow_each_key( VALUE self);
static VALUE pgrow_each_value( VALUE self);
static VALUE pgrow_to_hash( VALUE self);



VALUE rb_cPGRow;



VALUE
pgrow_init( self, keys)
    VALUE self, keys;
{
    VALUE len;
    
    len = LONG2NUM( RARRAY( keys)->len);
    rb_call_super( 1, &len);
    rb_ivar_set( self, id_keys, keys);
    return self;
}

/*
 * call-seq:
 *   row[position] -> value
 *   row[name] -> value
 *
 * Access elements of this row by column position or name.
 */
VALUE
pgrow_aref( argc, argv, self)
    int    argc;
    VALUE *argv;
    VALUE  self;
{
    if (argc == 1 && TYPE( argv[0]) == T_STRING) {
        VALUE keys = pgrow_keys( self);
        VALUE index = rb_funcall( keys, rb_intern( "index"), 1, argv[0]);
        if (index == Qnil) {
            rb_raise( rb_ePGError, "%s: field not found",
                            STR2CSTR( argv[0]));
        }
        else {
            return rb_ary_entry( self, NUM2INT( index));
        }
    }
    else {
        return rb_call_super( argc, argv);
    }
}

/*
 * call-seq:
 *   row.keys -> Array
 *
 * Column names.
 */
VALUE
pgrow_keys( self)
    VALUE self;
{
    return rb_ivar_get( self, id_keys);
}

/*
 * call-seq:
 *   row.values -> row
 */
VALUE
pgrow_values( self)
    VALUE self;
{
    return self;
}

/*
 * call-seq:
 *   row.each { |column,value| block } -> row
 *   row.each { |value| block } -> row
 *
 * Iterate with values or (column, value) pairs.
 */
VALUE
pgrow_each( self)
    VALUE self;
{
    int arity = NUM2INT( rb_funcall( rb_block_proc(), rb_intern( "arity"), 0));
    if (arity == 2)
        pgrow_each_pair( self);
    else
        pgrow_each_value( self);
    return self;
}

/*
 * call-seq:
 *   row.each_pair { |(column,value)| block } -> row
 *
 * Iterate with column, value pairs.
 */
VALUE
pgrow_each_pair( self)
    VALUE self;
{
    VALUE keys;
    int i;

    keys = pgrow_keys( self);
    for (i = 0; i < RARRAY( keys)->len; ++i)
        rb_yield( rb_assoc_new( rb_ary_entry( keys, i),
                                                rb_ary_entry( self, i)));
    return self;
}

/*
 * call-seq:
 *   row.each_key { |column| block } -> row
 *
 * Iterate with column names.
 */
VALUE
pgrow_each_key( self)
    VALUE self;
{
    rb_ary_each( pgrow_keys( self));
    return self;
}

/*
 * call-seq:
 *   row.each_value { |value| block } -> row
 *
 * Iterate with values.
 */
VALUE
pgrow_each_value( self)
    VALUE self;
{
    rb_ary_each( self);
    return self;
}

/*
 * call-seq:
 *   row.to_hash -> Hash
 *
 * Returns a +Hash+ of the row's values indexed by column name.
 * Equivalent to <tt>Hash [*row.keys.zip( row).flatten]</tt>
 */
VALUE
pgrow_to_hash( self)
    VALUE self;
{
    VALUE keys;
    int i;
    VALUE result;

    result = rb_hash_new();
    keys = pgrow_keys( self);
    for (i = 0; i < RARRAY( self)->len; ++i) {
        rb_hash_aset( result, rb_ary_entry( keys, i), rb_ary_entry( self, i));
    }
    return result;
}



/********************************************************************
 *
 * Document-class: Pg::Row
 *
 * Array subclass that provides hash-like behavior.
 */


void init_pg_row( void)
{
    rb_cPGRow = rb_define_class_under( rb_mPg, "Row", rb_cArray);
    rb_define_method( rb_cPGRow, "initialize", pgrow_init, 1);
    rb_define_method( rb_cPGRow, "[]", pgrow_aref, -1);
    rb_define_method( rb_cPGRow, "keys", pgrow_keys, 0);
    rb_define_method( rb_cPGRow, "values", pgrow_values, 0);
    rb_define_method( rb_cPGRow, "each", pgrow_each, 0);
    rb_define_method( rb_cPGRow, "each_pair", pgrow_each_pair, 0);
    rb_define_method( rb_cPGRow, "each_key", pgrow_each_key, 0);
    rb_define_method( rb_cPGRow, "each_value", pgrow_each_value, 0);
    rb_define_method( rb_cPGRow, "to_hash", pgrow_to_hash, 0);

    id_keys = rb_intern( "@keys");
}

