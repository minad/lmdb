require 'mkmf'

# Embed lmdb if we cannot find it
def have_lmbd
  find_header('lmdb.h') && have_library('lmdb', 'mdb_env_create')
end

if enable_config("bundled-lmdb", false) || !have_lmbd
  $VPATH << "$(srcdir)/liblmdb"
  $INCFLAGS << " -I$(srcdir)/liblmdb"
  $srcs = Dir.glob("#{$srcdir}/{,liblmdb/}*.c").map {|n| File.basename(n) }

  have_header 'limits.h'
  have_header 'string.h'
  have_header 'stdlib.h'
  have_header 'errno.h'
  have_header 'sys/types.h'
  have_header 'assert.h'

  have_header 'ruby.h'
  find_header 'lmdb.h'
end

create_makefile('lmdb_ext')
