//go:build tool

package main

import (
	"context"
	"html/template"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
)

var queueTemplate = `package main

/*
#cgo {{.Goos}} CFLAGS: -Ifdw -I{{.IncludeDir}}/server -I{{.IncludeDir}}/internal -g
#include "postgres.h"
#include "common.h"
#include "fdw_helpers.h"
*/
import "C"

/**

This file is tactically named so that it gets read and compiled at the start of the build chain.

This file includes the necessary libs and headers required for the compilation of the rest
of the project

**/
`

type data struct {
	Goos       string
	IncludeDir string
}

func main() {
	ctx := context.Background()

	cmd := exec.CommandContext(ctx, "pg_config", "--includedir")
	output, err := cmd.Output()
	if err != nil {
		panic(err)
	}
	includeDir := string(output)
	includeDir = strings.TrimSpace(includeDir)

	outputFile, err := os.Create("./0_prebuild.go")
	if err != nil {
		panic(err)
	}
	defer outputFile.Close()

	writer := io.MultiWriter(os.Stdout, outputFile)

	t := template.Must(template.New("prebuild").Parse(queueTemplate))
	t.Execute(writer, data{
		Goos:       runtime.GOOS,
		IncludeDir: string(includeDir),
	})
}
