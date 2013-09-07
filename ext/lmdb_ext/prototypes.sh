#!/bin/bash
ctags -f - lmdb_ext.c | grep -P '\tf\t|f$' | sed -e 's#.*/\^##' -e 's#\$/.*##' -e 's# {#;#' > prototypes.h
sed -i -ne '/BEGIN PROTOTYPES/ {p; r prototypes.h' -e ':a; n; /END PROTOTYPES/ {p; b}; ba}; p' lmdb_ext.h
rm prototypes.h
