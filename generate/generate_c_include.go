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

var queueTemplate = `package main

/*
#cgo {{.Goos}} CFLAGS: -Ifdw -I{{.ServerIncludeDir}} -I{{.InternalIncludeDir}} -g
#include "postgres.h"
#include "common.h"
#include "fdw_helpers.h"
*/
import "C"

/**

This file is tactically named so that it gets read and compiled at the start of the build chain.

This is generated on make or by running go generate ./generate in the repository root and as such
does not need to be check in to version control

It includes the necessary libs and headers required for the compilation of the rest
of the project

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
