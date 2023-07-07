# Makefile

PLATFORM=$(shell uname)

install: build
	if test -d ~/.steampipe/db/14.2.0; then \
		cp ./build-$(PLATFORM)/steampipe_postgres_fdw--1.0.sql ~/.steampipe/db/14.2.0/postgres/share/postgresql/extension/; \
		cp ./build-$(PLATFORM)/steampipe_postgres_fdw.control ~/.steampipe/db/14.2.0/postgres/share/postgresql/extension/; \
		cp ./build-$(PLATFORM)/steampipe_postgres_fdw.so ~/.steampipe/db/14.2.0/postgres/lib/postgresql/; \
	fi

build: prebuild.go
	$(MAKE) -C ./fdw clean
	$(MAKE) -C ./fdw go
	$(MAKE) -C ./fdw
	$(MAKE) -C ./fdw inst
	
	rm -f prebuild.go

prebuild.go:
	cp prebuild.tmpl prebuild.go
	sed -i.bak 's|OS_PLACEHOLDER|$(shell go env GOOS)|' prebuild.go
	sed -i.bak 's|INTERNAL_INCLUDE_PLACEHOLDER|$(shell pg_config --includedir)|' prebuild.go
	sed -i.bak 's|SERVER_INCLUDE_PLACEHOLDER|$(shell pg_config --includedir-server)|' prebuild.go
	sed -i.bak 's|DISCLAIMER|This is generated. Do not check this in to Git|' prebuild.go
	rm -f prebuild.go.bak

clean:
	$(MAKE) -C ./fdw clean
	rm -f prebuild.go
	rm -f steampipe_postgres_fdw.a
	rm -f steampipe_postgres_fdw.h

# Used to build the Darwin ARM binaries and upload to the github draft release.
# Usage: make release input="v1.7.2"
release:
	./scripts/upload_arm_asset.sh $(input)
