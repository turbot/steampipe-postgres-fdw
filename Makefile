# Makefile
default: build

STEAMPIPE_INSTALL_DIR ?= ~/.steampipe

# The platform follows the target OS (GOOS), not the host, so a Darwin build
# cross-compiled on Linux lands in build-Darwin.
ifeq ($(shell go env GOOS), darwin)
PLATFORM = Darwin
else
PLATFORM = Linux
endif

GETTEXT_INCLUDE=$(shell dirname $(shell dirname $(shell readlink -f $(shell which gettext))))/include

install: build
	if test -d ~/.steampipe/db/14.17.0; then \
		cp ./build-$(PLATFORM)/steampipe_postgres_fdw--1.0.sql $(STEAMPIPE_INSTALL_DIR)/db/14.17.0/postgres/share/postgresql/extension/; \
		cp ./build-$(PLATFORM)/steampipe_postgres_fdw.control $(STEAMPIPE_INSTALL_DIR)/db/14.17.0/postgres/share/postgresql/extension/; \
		cp ./build-$(PLATFORM)/steampipe_postgres_fdw.so $(STEAMPIPE_INSTALL_DIR)/db/14.17.0/postgres/lib/postgresql/; \
	fi

# build standalone: render the code for the plugin, then build it
standalone: render
	@echo "Building standalone FDW for plugin: $(plugin)"
	$(MAKE) build_from_work

# Cross-compile the standalone FDW for macOS on Linux, in the goreleaser-cross
# image. Fetches the pinned Homebrew Postgres headers for pg_major (14 or 15)
# and GOARCH, then runs the standalone target against them. Set GOOS=darwin,
# GOARCH, CGO_ENABLED=1, CC (oa64-clang or o64-clang) and
# MACOSX_DEPLOYMENT_TARGET in the environment.
DARWIN_CROSS_DIR = $(CURDIR)/darwin-cross

standalone_darwin_cross: validate_plugin validate_version validate_pg_major
	@if [ "$$(go env GOOS)" != darwin ]; then echo "Set GOOS=darwin for $@" >&2; exit 1; fi
	@if [ "$$(go env GOHOSTOS)" = darwin ]; then echo "$@ is for building on Linux; on macOS, use make standalone" >&2; exit 1; fi
	scripts/darwin_cross_deps.sh $(pg_major) $$(go env GOARCH) $(DARWIN_CROSS_DIR)

	# prebuild.go holds the Postgres include paths, so render it afresh
	rm -f prebuild.go
	PATH="$(DARWIN_CROSS_DIR)/bin:$$PATH" $(MAKE) standalone GETTEXT_INCLUDE=$(DARWIN_CROSS_DIR)/intl

# render target
render: validate_plugin validate_version prebuild.go
	@echo "Rendering code for plugin: $(plugin)"

	# Remove existing work dir and create a new directory for the render process
	rm -rf work && \
	mkdir -p work

	# Copy the source tree into the new directory (tar rather than rsync, which
	# the goreleaser-cross image doesn't have)
	tar -cf - --exclude=./.git --exclude=./work --exclude=./darwin-cross . | tar -xf - -C work

	# Change to the new directory to perform operations. The generator runs on
	# the build host, so it's built for the host even when GOOS/GOARCH are set
	# for a cross build (e.g. GOOS=darwin on Linux).
	cd work && \
	GOOS=$$(go env GOHOSTOS) GOARCH=$$(go env GOHOSTARCH) CGO_ENABLED=0 go run generate/generator.go templates . $(plugin) $(plugin_version) $(plugin_github_url) && \
	if [ ! -z "$(plugin_version)" ]; then \
		MAJOR_VERSION=$$(echo $(plugin_version) | grep -o '^v[0-9]\+' || echo "v1") && \
		if [ "$$MAJOR_VERSION" = "v1" ] || [ "$$MAJOR_VERSION" = "v0" ]; then \
			echo "go get $(plugin_github_url)@$(plugin_version)" && \
			go get $(plugin_github_url)@$(plugin_version); \
		else \
			echo "go get $(plugin_github_url)/$$MAJOR_VERSION@$(plugin_version)" && \
			go get $(plugin_github_url)/$$MAJOR_VERSION@$(plugin_version); \
		fi; \
	fi && \
	go mod tidy

	# Note: The work directory will contain the full code tree with rendered changes

# build_from_work target
build_from_work:
	@if [ ! -d "work" ]; then \
		echo "Error: 'work' directory does not exist. Please run the render target first." >&2; \
		exit 1; \
	fi
	@echo "Building from work directory for plugin: $(plugin)"

	# Change to the work directory to perform build operations
	cd work && \
	$(MAKE) -C ./fdw clean && \
	$(MAKE) -C ./fdw go && \
	$(MAKE) -C ./fdw && \
	$(MAKE) -C ./fdw standalone

	# Delete existing build-${PLATFORM} and copy the binaries to the actual 
	# build-${PLATFORM} folder
	rm -rf build-${PLATFORM} && \
	mkdir -p build-${PLATFORM} && \
	cp -a work/build-${PLATFORM}/* build-${PLATFORM}/

	# Note: This target builds from the 'work' directory and copies binaries to the build-${PLATFORM} folder

validate_plugin:
ifndef plugin
	$(error "The 'plugin' variable is missing. Usage: make build plugin=<plugin_name> [plugin_version=<version>] [plugin_github_url=<url>]")
endif

# Check if plugin_github_url is provided when plugin_version is specified
validate_version:
ifdef plugin_version
ifndef plugin_github_url
	$(error "The 'plugin_github_url' variable is required when 'plugin_version' is specified")
endif
endif

validate_pg_major:
ifeq ($(filter 14 15,$(pg_major)),)
	$(error "The 'pg_major' variable must be 14 or 15")
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
	rm -rf work darwin-cross

# Used to build the Darwin ARM binaries and upload to the github draft release.
# Usage: make release input="v1.7.2"
release:
	./scripts/upload_arm_asset.sh $(input)
