# fdw/Makefile

MODULE_big = steampipe_postgres_fdw
OBJS = datum.o query.o fdw.o logging.o

SHLIB_LINK = steampipe_postgres_fdw.a

# The platform follows the target OS (GOOS), not the host, so a Darwin build
# cross-compiled on Linux gets the Darwin flags and lands in build-Darwin.
GOOS := $(shell go env GOOS)
GOHOSTOS := $(shell go env GOHOSTOS)

ifeq ($(GOOS), darwin)
	PLATFORM = Darwin
	PG_LDFLAGS=-framework Foundation -framework AppKit -framework Security
else
	PLATFORM = Linux
endif

EXTENSION = steampipe_postgres_fdw
DATA = steampipe_postgres_fdw--1.0.sql

REGRESS = steampipe_postgres-fdw

EXTRA_CLEAN = steampipe_postgres_fdw.a fdw.h

PG_CONFIG = pg_config

ifeq ($(GOOS), $(GOHOSTOS))

PGXS := $(shell $(PG_CONFIG) --pgxs)
SERVER_LIB = $(shell $(PG_CONFIG) --includedir)/server
INTERNAL_LIB = $(shell $(PG_CONFIG) --includedir)/internal

ifeq ($(GOOS), darwin)
	SERVER_LIB = $(shell $(PG_CONFIG) --includedir)/postgresql/server
	INTERNAL_LIB = $(shell $(PG_CONFIG) --includedir)/postgresql/internal
endif

PG_CFLAGS = -I${SERVER_LIB} -I${INTERNAL_LIB} -g

include $(PGXS)

else ifeq ($(GOOS), darwin)

# Cross build for macOS (e.g. on Linux in goreleaser-cross). PGXS only builds
# for the host, so these rules do what PGXS does on macOS, using the target's
# pg_config from scripts/darwin_cross_deps.sh.
SERVER_LIB = $(shell $(PG_CONFIG) --includedir-server)
INTERNAL_LIB = $(shell $(PG_CONFIG) --pkgincludedir)/internal

PG_CFLAGS = -I${SERVER_LIB} -I${INTERNAL_LIB} -g

CROSS_CFLAGS = $(shell $(PG_CONFIG) --cflags) $(PG_CFLAGS)
CROSS_CPPFLAGS = -I. -I./ -I$(SERVER_LIB) -I$(INTERNAL_LIB) $(shell $(PG_CONFIG) --cppflags)

all: $(MODULE_big).so

%.o: %.c
	$(CC) $(CROSS_CFLAGS) $(CROSS_CPPFLAGS) -c -o $@ $<

# As PGXS links a loadable module on macOS: a bundle whose Postgres symbols
# resolve against the postgres executable that loads it.
$(MODULE_big).so: $(OBJS)
	$(CC) $(CROSS_CFLAGS) -bundle -o $@ $(OBJS) $(PG_LDFLAGS) $(shell $(PG_CONFIG) --ldflags) \
		$(SHLIB_LINK) -bundle_loader $(shell $(PG_CONFIG) --bindir)/postgres

clean:
	rm -f $(MODULE_big).so $(OBJS) $(EXTRA_CLEAN)

else
$(error Cross-compiling the FDW is only supported for GOOS=darwin)
endif

# Always enable netgo for the build
BUILD_TAGS = netgo

ifeq ($(GOOS), darwin)
	export CGO_LDFLAGS = -Wl,-undefined,dynamic_lookup
endif

# if we are building for pg16, we need to use the pg10 build tags
ifeq ($(shell $(PG_CONFIG) --version | cut -d' ' -f2 | cut -d'.' -f1), 14)
	BUILD_TAGS := $(BUILD_TAGS),pg14
endif

go: ../fdw.go
	@echo $(BUILD_TAGS)
	# we are building with the net package from go
	# this has the caveat that, since we are not binding to lresolv, DNS resolution may 
	# have some subtle differences from system DNS resolution 
	CGO_ENABLED=1 go build -v -o steampipe_postgres_fdw.a  -tags "$(BUILD_TAGS)" -buildmode=c-archive ../*.go

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