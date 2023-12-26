package main

/*
#cgo linux LDFLAGS: -Wl,-unresolved-symbols=ignore-all
#cgo darwin LDFLAGS: -Wl,-undefined,dynamic_lookup
#include "fdw_helpers.h"
*/
import "C"

import (
	_ "github.com/aquasecurity/trivy-db/pkg/types"
	_ "github.com/aquasecurity/trivy/pkg/commands/artifact"
	_ "github.com/aquasecurity/trivy/pkg/commands/option"
	_ "github.com/aquasecurity/trivy/pkg/types"
)

//export goDummy
func goDummy() {
}

// required by buildmode=c-archive
func main() {}
