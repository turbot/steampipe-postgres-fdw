# Makefile

build: 0_prebuild.go
	$(MAKE) -C ./fdw clean
	$(MAKE) -C ./fdw go
	$(MAKE) -C ./fdw
	$(MAKE) -C ./fdw inst
	
	rm -f 0_prebuild.go

0_prebuild.go:
	go generate ./...
	
