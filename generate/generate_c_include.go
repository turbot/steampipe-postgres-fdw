//go:build tool

package main

import (
	"context"
	"fmt"
	"html/template"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
)

// This program is used to generate a single go file with the necessary include directives for postgres libraries
// It uses 'pg_config' to locate the libraries that are necessary to build the FDW and creates a go source file
// which is used by cgo to refer to the postgres libraries.
//
// This is done with a go generator, since it is non-trivial to generate template based output with 'make'
// and we already have access to the golang eco-system in this project
//
// This is invoked by 'make'
var queueTemplate = `package main

/*
#cgo {{.Goos}} CFLAGS: -Ifdw -I{{.ServerIncludeDir}} -I{{.InternalIncludeDir}} -g
#include "postgres.h"
#include "common.h"
#include "fdw_helpers.h"
*/
import "C"

/**
This is auto-generated and does not need to be checked in to version control
It provides the directive to include the necessary libs and headers required for the compilation of the rest of the project
**/
`

type data struct {
	Goos               string
	ServerIncludeDir   string
	InternalIncludeDir string
}

func main() {
	ctx := context.Background()

	cmd := exec.CommandContext(ctx, "pg_config", "--includedir")
	output, err := cmd.Output()
	if err != nil {
		panic(err)
	}
	includeDir := strings.TrimSpace(string(output))

	cmd = exec.CommandContext(ctx, "pg_config", "--includedir-server")
	output, err = cmd.Output()
	if err != nil {
		panic(err)
	}
	serverIncludeDir := string(output)
	serverIncludeDir = strings.TrimSpace(serverIncludeDir)

	// the file should be tactically named so that it gets read and compiled at the start of the build chain.
	// this is necessary since this file will contain the necessary includes that the rest of the build needs to
	// work properly
	outputFile, err := os.Create("./0_prebuild.go")
	if err != nil {
		panic(err)
	}
	defer outputFile.Close()

	writer := io.MultiWriter(os.Stdout, outputFile)

	t := template.Must(template.New("prebuild").Parse(queueTemplate))
	t.Execute(writer, data{
		Goos:               runtime.GOOS,
		InternalIncludeDir: fmt.Sprintf("%s/internal", includeDir),
		ServerIncludeDir:   serverIncludeDir,
	})
}
