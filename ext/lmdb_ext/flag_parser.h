static int METHOD(VALUE key, VALUE value, int* flags) {
        ID id = rb_to_id(key);

        if (0) {}
#define FLAG(const, name) else if (id == rb_intern(#name)) { if (RTEST(value)) { *flags |= MDB_##const; } }
#include FILE
#undef FLAG
        else {
                VALUE s = rb_inspect(key);
                rb_raise(cError, "Invalid option %s", StringValueCStr(s));
        }

        return 0;
}
