# fdw/Makefile

MODULE_big = steampipe_postgres_fdw
OBJS = datum.o query.o fdw.o logging.o

SHLIB_LINK =  steampipe_postgres_fdw.a

ifeq ($(shell uname), Darwin)
	PG_LDFLAGS=-framework Foundation -framework AppKit -framework Security
endif

# if [ "$(uname)" == "Darwin" ]; then
# 	PG_LDFLAGS="-framework Foundation -framework AppKit -framework Security";
# fi;

PG_CFLAGS = -I./include/postgresql/server -I./include/postgresql/internal

EXTENSION = steampipe_postgres_fdw
DATA = steampipe_postgres_fdw--1.0.sql

REGRESS = steampipe_postgres-fdw

EXTRA_CLEAN =  steampipe_postgres_fdw.a fdw.h

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

go: ../fdw.go
	go build -o  steampipe_postgres_fdw.a -buildmode=c-archive ../*.go

inst:
	cp steampipe_postgres_fdw.so ~/.steampipe/db/12.1.0/postgres/lib/postgresql/steampipe_postgres_fdw.so
	cp steampipe_postgres_fdw.control ~/.steampipe/db/12.1.0/postgres/share/postgresql/extension/steampipe_postgres_fdw.control
	cp steampipe_postgres_fdw--1.0.sql ~/.steampipe/db/12.1.0/postgres/share/postgresql/extension/steampipe_postgres_fdw--1.0.sql
