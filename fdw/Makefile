# fdw/Makefile

MODULE_big = steampipe_postgres_fdw
OBJS = datum.o query.o fdw.o logging.o

SHLIB_LINK = steampipe_postgres_fdw.a

PLATFORM=$(shell uname)

ifeq ($(shell uname), Darwin)
	PG_LDFLAGS=-framework Foundation -framework AppKit -framework Security
endif

EXTENSION = steampipe_postgres_fdw
DATA = steampipe_postgres_fdw--1.0.sql

REGRESS = steampipe_postgres-fdw

EXTRA_CLEAN = steampipe_postgres_fdw.a fdw.h

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
SERVER_LIB = $(shell $(PG_CONFIG) --includedir)/server
INTERNAL_LIB = $(shell $(PG_CONFIG) --includedir)/internal

ifeq ($(shell uname), Darwin)
	SERVER_LIB = $(shell $(PG_CONFIG) --includedir)/postgresql/server
	INTERNAL_LIB = $(shell $(PG_CONFIG) --includedir)/postgresql/internal
endif

PG_CFLAGS = -I${SERVER_LIB} -I${INTERNAL_LIB} -g

include $(PGXS)

go: ../fdw.go
	go1.19rc1 build -o steampipe_postgres_fdw.a -buildmode=c-archive ../*.go

inst:
	mkdir -p ../build-${PLATFORM}
	rm -f ../build-${PLATFORM}/*

	cp steampipe_postgres_fdw.so ../build-${PLATFORM}
	cp steampipe_postgres_fdw.control ../build-${PLATFORM}
	cp steampipe_postgres_fdw--1.0.sql ../build-${PLATFORM}
	
	rm steampipe_postgres_fdw.so
	rm steampipe_postgres_fdw.a
	rm steampipe_postgres_fdw.h
	
	rm ./*.o
	
release:
	./../upload_arm_asset.sh
