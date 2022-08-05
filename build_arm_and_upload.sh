#!/usr/bin/env bash

# This script is used to build the ARM binaries and upload to the github draft release.
# Just run this script from your Mac M1 or Linux ARM machine, if you want to build and upload the ARM binaries.

cd fdw
make clean
make go
make
make release
cd -