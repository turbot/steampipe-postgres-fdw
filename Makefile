# Makefile

STEAMPIPE_INSTALL_DIR ?= ~/.steampipe

PLATFORM=$(shell uname)
GETTEXT_INCLUDE=$(shell dirname $(shell dirname $(shell readlink -f $(shell which gettext))))/include

install: build
	if test -d ~/.steampipe/db/14.2.0; then \
		cp ./build-$(PLATFORM)/steampipe_postgres_fdw--1.0.sql $(STEAMPIPE_INSTALL_DIR)/db/14.2.0/postgres/share/postgresql/extension/; \
		cp ./build-$(PLATFORM)/steampipe_postgres_fdw.control $(STEAMPIPE_INSTALL_DIR)/db/14.2.0/postgres/share/postgresql/extension/; \
	fi
	
	if test -f ./build-$(PLATFORM)/steampipe_postgres_fdw.so; then \
		cp ./build-$(PLATFORM)/steampipe_postgres_fdw.so $(STEAMPIPE_INSTALL_DIR)/db/14.2.0/postgres/lib/postgresql/; \
	fi
	if test -f ./build-$(PLATFORM)/steampipe_postgres_fdw.dylib; then \
		cp ./build-$(PLATFORM)/steampipe_postgres_fdw.dylib $(STEAMPIPE_INSTALL_DIR)/db/14.2.0/postgres/lib/postgresql/; \
	fi

# build standalone 
standalone: validate_plugin prebuild.go
	@echo "Building standalone FDW for plugin: $(plugin)"
	go run generate/generator.go templates . $(plugin) $(plugin_github_url)
	go mod tidy
	$(MAKE) -C ./fdw clean
	$(MAKE) -C ./fdw go
	$(MAKE) -C ./fdw
	$(MAKE) -C ./fdw standalone
	
	rm -f prebuild.go

validate_plugin:
    ifndef plugin
	    $(error "You must specify the 'plugin' variable")
    endif

build: prebuild.go
	$(MAKE) -C ./fdw clean
	$(MAKE) -C ./fdw go
	$(MAKE) -C ./fdw
	$(MAKE) -C ./fdw inst
	
	rm -f prebuild.go

# make target to generate a go file containing the C includes containing bindings to the
# postgres functions
prebuild.go:
	# copy the template which contains the C includes
	# this is used to import the postgres bindings by the underlying C compiler
	cp prebuild.tmpl prebuild.go
	
	# set the GOOS in the template 
	sed -i.bak 's|OS_PLACEHOLDER|$(shell go env GOOS)|' prebuild.go
	
	# replace known placeholders with values from 'pg_config'
	sed -i.bak 's|INTERNAL_INCLUDE_PLACEHOLDER|$(shell pg_config --includedir)|' prebuild.go
	sed -i.bak 's|SERVER_INCLUDE_PLACEHOLDER|$(shell pg_config --includedir-server)|' prebuild.go
	sed -i.bak 's|DISCLAIMER|This is generated. Do not check this in to Git|' prebuild.go
	sed -i.bak 's|LIB_INTL_PLACEHOLDER|$(GETTEXT_INCLUDE)|' prebuild.go
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

