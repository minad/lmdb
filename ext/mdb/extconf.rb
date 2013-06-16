require 'mkmf'

dir_config 'liblmdb'

# Embed lmdb if we cannot find it
unless find_header('lmdb.h')
  $VPATH << "$(srcdir)/liblmdb"
  $INCFLAGS << " -I$(srcdir)/liblmdb"
  $srcs = Dir.glob("#{$srcdir}/{,liblmdb/}*.c").map {|n| File.basename(n) }

  find_header 'lmdb.h'
end

create_makefile('mdb_ext')
