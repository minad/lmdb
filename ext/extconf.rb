require 'mkmf'
find_header 'lmdb.h', 'liblmdb'
$objs = Dir['**/*.c'].to_a.map {|s| s.sub(/\.c$/, '.o') }
dir_config('mdb')
create_makefile('mdb')
