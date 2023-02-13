# Makefile

PLATFORM=$(shell uname)

install: build
	if test -d ~/.steampipe/db/14.2.0; then \
		cp ./build-$(PLATFORM)/steampipe_postgres_fdw--1.0.sql ~/.steampipe/db/14.2.0/postgres/share/postgresql/extension/; \
		cp ./build-$(PLATFORM)/steampipe_postgres_fdw.control ~/.steampipe/db/14.2.0/postgres/share/postgresql/extension/; \
		cp ./build-$(PLATFORM)/steampipe_postgres_fdw.so ~/.steampipe/db/14.2.0/postgres/lib/postgresql/; \
	fi

build: 0_prebuild.go
	$(MAKE) -C ./fdw clean
	$(MAKE) -C ./fdw go
	$(MAKE) -C ./fdw
	$(MAKE) -C ./fdw inst
	
	rm -f 0_prebuild.go

0_prebuild.go:
	go generate ./...

clean:
	$(MAKE) -C ./fdw clean
	rm -f 0_prebuild.go
	rm -f steampipe_postgres_fdw.a
	rm -f steampipe_postgres_fdw.h
	