#!/usr/bin/env bash

# This script is used to build FDW binaries for Darwin and Linux. Should be used in release
# workflows. Running this locally would just create the binaries, not update them in your system.

cd fdw
make clean
make go
make
make inst
cd -

#pg_ctl -D /usr/local/var/postgres restart
#psql postgres
