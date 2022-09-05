#!/usr/bin/env bash

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
gzip steampipe_postgres_fdw.so
mv steampipe_postgres_fdw.so.gz steampipe_postgres_fdw.so.darwin_arm64.gz
gh release upload $1 steampipe_postgres_fdw.so.darwin_arm64.gz
