#!/usr/bin/env bash

# This script is used to build the ARM binaries and upload to the github draft release.
# Just run this script from your Mac M1 or Linux ARM machine, if you want to build and upload the ARM binaries.
# This script requires the tag to be built as an argument(eg: ./build_arm_and_upload.sh v1.4.0-rc.2 where v1.4.0-rc.2
# is the tag to be built.)

# Pull and checkout the tag
git checkout main
git pull
git checkout $1

# Build from the tag
make clean
make

# Run the upload script
./scripts/upload_arm_asset.sh $1
