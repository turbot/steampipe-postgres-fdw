# Attempt at building for M1 from x86_64

`Dockerfile`:
```Dockerfile
FROM goreleaser/goreleaser-cross:latest

RUN mkdir -p /usr/local/include/postgresql/server
RUN mkdir -p /usr/local/include/postgresql/internal

COPY ./include/server /usr/local/include/postgresql/server
COPY ./include/internal /usr/local/include/postgresql/internal

ENTRYPOINT [ "/bin/bash" ]
```
 `docker build -t steampipe/fdw-builder .`

## Libraries
|||
|-|-|
| `./fdw/src` | ` /usr/local/lib/postgresql/pgxs` |
| `./include/server` | `/usr/local/Cellar/postgresql/14.2_1/include/postgresql/server` |
| `./include/internal` | `/usr/local/Cellar/postgresql/14.2_1/include/postgresql/internal` |
| `./include/MacOSX12.3.sdk` | `/Library/Developer/CommandLineTools/SDKs/MacOSX12.3.sdk` |

We need to copy these, since the ones pointed to by `$(pg_config --libdir)` are `symlinks`

Maybe we can `mount` the `Cellar` directories directly. Not tested.

## Updated `fdw/Makefile`
```Makefile
# fdw/Makefile

MODULE_big = steampipe_postgres_fdw
OBJS = datum.o query.o fdw.o logging.o

SHLIB_LINK = steampipe_postgres_fdw.a

PLATFORM=$(shell uname)

EXTENSION = steampipe_postgres_fdw
DATA = steampipe_postgres_fdw--1.0.sql

REGRESS = steampipe_postgres-fdw

EXTRA_CLEAN = steampipe_postgres_fdw.a fdw.h

SERVER_LIB = /usr/local/include/postgresql/server
INTERNAL_LIB = /usr/local/include/postgresql/internal
PG_SYSROOT=/sysroot/macos/amd64
PG_CFLAGS = -I${SERVER_LIB} -I${INTERNAL_LIB} -g

PGXS=./src/makefiles/pgxs.mk
include ${PGXS}

go: ../fdw.go
	@echo $(CC)
	@echo $(CXX)
	go env
	go build -v -x -o steampipe_postgres_fdw.a -buildmode=c-archive ../*.go 

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
	

```

## Command
```bash
GOOS=darwin GOARCH=amd64 CGO_ENABLED=1 CC=o64-clang CXX=o64-clang++  OSXCROSS_NO_INCLUDE_PATH_WARNINGS=1 PG_SYSROOT=../include/MacOSX12.3.sdk make go -e
```
```bash
GOOS=darwin GOARCH=amd64 CGO_ENABLED=1 CC=o64-clang CXX=o64-clang++  OSXCROSS_NO_INCLUDE_PATH_WARNINGS=1 PG_SYSROOT=../include/MacOSX12.3.sdk make -e
```