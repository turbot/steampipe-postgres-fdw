#!/usr/bin/env bash

# This script is used to build FDW binaries for Linux ARM. This script is used in 
# the build-linux-arm job in Build Draft Release workflow to ssh into the ec2
# instance and build the binaries.

#function that makes the script exit, if any command fails
exit_if_failed () {
if [ $? -ne 0 ]
then
  exit 1
fi
}

echo "Check arch and export GOROOT & GOPATH"
uname -m
export GOROOT=/usr/local/go
export PATH=$GOPATH/bin:$GOROOT/bin:$PATH
echo ""

echo "Try an existing file"
cat test.txt
exit_if_failed
echo ""

echo "Check go version"
go version
exit_if_failed
echo ""

echo "Checkout to cloned fdw anywhere repo"
cd steampipe-postgres-fdw-anywhere
pwd
echo ""

echo "git reset"
git reset
exit_if_failed
echo ""

echo "git restore all changed files(if any)"
git restore .
exit_if_failed
echo ""

echo "git fetch"
git fetch
exit_if_failed
echo ""

echo "git pull origin main"
git checkout main
git pull origin main
exit_if_failed
echo ""

echo "git checkout <tag>"
input=$1
echo $input
git checkout $input
git branch --list
exit_if_failed
echo ""

echo "Remove existing build-Linux dir"
rm -rf build-Linux
exit_if_failed
echo ""

echo "Run build"
make
exit_if_failed
echo ""

echo "Check if binary is created"
file build-Linux/steampipe_postgres_fdw.so
exit_if_failed
echo ""

echo "Hallelujah!"
exit 0