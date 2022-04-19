#!/usr/bin/env bash
cd fdw
make clean
make go
make
make release
cd -