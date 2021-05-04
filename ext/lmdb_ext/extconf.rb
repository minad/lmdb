require 'mkmf'

$CFLAGS << ' -std=c99 -Wall -g '

# Embed lmdb if we cannot find it
if enable_config("bundled-lmdb", false) || !(find_header('lmdb.h') && have_library('lmdb', 'mdb_env_create'))
  $INCFLAGS << " -I$(srcdir)/liblmdb"
  $VPATH ||= []
  $VPATH << "$(srcdir)/liblmdb"
  $srcs = Dir.glob("#{$srcdir}/{,liblmdb/}*.c").map {|n| File.basename(n) }
end

have_header 'limits.h'
have_header 'string.h'
have_header 'stdlib.h'
have_header 'errno.h'
have_header 'sys/types.h'
have_header 'assert.h'

have_header 'ruby.h'
have_func 'rb_funcall_passing_block'
have_func 'rb_thread_call_without_gvl2'

create_header

create_makefile('lmdb_ext')
