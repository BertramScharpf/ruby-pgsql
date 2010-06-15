/*
 *  module.c  --  Pg module
 */


#include "module.h"


VALUE rb_mPg;
VALUE rb_ePGError;


void init_pg_module( void)
{
    rb_mPg = rb_define_module( "Pg");

    rb_define_const( rb_mPg, "VERSION", rb_obj_freeze( rb_str_new2( VERSION)));

    rb_ePGError = rb_define_class_under( rb_mPg, "Error", rb_eStandardError);
}

