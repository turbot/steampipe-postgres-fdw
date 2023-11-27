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

# Determine the operating system
OS := $(shell uname)

# Check if the OS is Mac OS/Darwin
ifeq ($(OS),Darwin)
  BUILD_TAGS = netgo
endif

ifeq ($(shell uname -s),Darwin)
	export CGO_LDFLAGS = -Wl,-undefined,dynamic_lookup
endif

go: ../fdw.go
	# we are building with the net package from go
	# this has the caveat that, since we are not binding to lresolv, DNS resolution may 
	# have some subtle differences from system DNS resolution 
	CGO_ENABLED=1 go build -v -x -o steampipe_postgres_fdw.a  -tags "$(BUILD_TAGS)" -buildmode=c-archive ../*.go

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

standalone:
	mkdir -p ../build-${PLATFORM}
	rm -f ../build-${PLATFORM}/*

	cp steampipe_postgres_fdw.so ../build-${PLATFORM}/steampipe_postgres_$(plugin).so
	cp steampipe_postgres_fdw.control ../build-${PLATFORM}/steampipe_postgres_$(plugin).control
	cp steampipe_postgres_fdw--1.0.sql ../build-${PLATFORM}/steampipe_postgres_$(plugin)--1.0.sql
	cp ../scripts/README.md ../build-${PLATFORM}/README.md
	cp ../scripts/install.sh ../build-${PLATFORM}/install.sh
	
	rm steampipe_postgres_fdw.so
	rm steampipe_postgres_fdw.a
	rm steampipe_postgres_fdw.h
	
	rm ./*.o