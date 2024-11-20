// Package version :: The version package provides a location to set the release versions for all
// packages to consume, without creating import cycles.
//
// This package should not import any other steampipe packages.
package version

import (
	"fmt"

	"github.com/Masterminds/semver/v3"
)

// The main version number that is being run at the moment.
var fdwVersion = "1.12.1"

// A pre-release marker for the version. If this is "" (empty string)
// then it means that it is a final release. Otherwise, this is a pre-release
// such as "dev" (in development), "beta", "rc1", etc.
var prerelease = ""

// FdwVersion is an instance of semver.Version. This has the secondary
// benefit of verifying during tests and init time that our version is a
// proper semantic version, which should always be the case.
var FdwVersion *semver.Version

var VersionString string

func init() {
	VersionString = fdwVersion
	if prerelease != "" {
		VersionString = fmt.Sprintf("%s-%s", fdwVersion, prerelease)
	}
	FdwVersion = semver.MustParse(VersionString)
}
