#!/usr/bin/env bash

# This script uploads the created binary to the draft release candidate.
# This is called from make release.

ARCH=$(uname -m)
# exit if the architecture is not arm64(darwin) or aarch64(linux)
if [[ "$ARCH" != "arm64" ]] && [[ "$ARCH" != "aarch64" ]]; then
  echo "Not an ARM64 system"
  exit
fi

# Must have these commands for the script to run
declare -a required_commands=("gh" "gzip" "postgres")

for required_command in "${required_commands[@]}"
do
  if [[ $(command -v $required_command | head -c1 | wc -c) -eq 0 ]]; then
    echo "$required_command is required for this script to run."
    exit -1
  fi
done

# Zip, rename and upload the binary
gzip build-Darwin/steampipe_postgres_fdw.so
mv build-Darwin/steampipe_postgres_fdw.so.gz build-Darwin/steampipe_postgres_fdw.so.darwin_arm64.gz
gh release upload $1 build-Darwin/steampipe_postgres_fdw.so.darwin_arm64.gz
