package main

/*
#cgo darwin CFLAGS: -Ifdw -I/usr/local/include/postgresql/server -I/usr/local/include/postgresql/internal -g
#cgo linux CFLAGS: -Ifdw -I/usr/include/postgresql/14/server -I/usr/include/postgresql/internal
#include "postgres.h"
#include "common.h"
#include "fdw_helpers.h"
*/
import "C"

/**

This file is tactically named so that it gets read and compiled at the start of the build chain.

This file includes the necessary libs and headers required for the compilation of the rest
of the project

**/
